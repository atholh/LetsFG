"""
easyJet Playwright connector — navigates to easyJet's deep-link flight search URL
and extracts structured data from window.appData.searchResult.

easyJet's API (/funnel/api/query) is behind Akamai WAF — requires browser-level
session. The direct deep-link URL doesn't reliably trigger the search (BFF
timeout without prior session cookies). Going through the homepage form works.

Strategy:
1. Navigate to easyjet.com/en/ homepage
2. Fill origin/destination/date in the search form
3. Click "Show flights" → navigates to /buy/flights
4. Wait for window.appData.searchResult
5. Parse journeyPairs → FlightOffers

Response data:  window.appData.searchResult.journeyPairs[0].outbound.flights
  → dict keyed by date, each value is a list of flight objects with fares
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import re
import time
from datetime import datetime
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "en-IE"]
_TIMEZONES = ["Europe/London", "Europe/Berlin", "Europe/Paris", "Europe/Madrid"]

_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Shared headless Chromium (launched once, reused across searches)."""
    global _pw_instance, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright
        _pw_instance = await async_playwright().start()
        try:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
        logger.info("easyJet: Playwright browser launched (headed Chrome)")
        return _browser


class EasyjetConnectorClient:
    """easyJet Playwright connector — homepage form search + window.appData extraction."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser is shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search easyJet using Playwright homepage form + window.appData extraction.

        The direct deep-link URL (/buy/flights?dep=...) doesn't reliably trigger
        the BFF search (times out without prior session cookies). Going through
        the homepage form works consistently.

        Strategy:
        1. Navigate to easyjet.com/en/ homepage
        2. Fill the search form (origin, destination, date, one-way)
        3. Click "Show flights" → navigates to /buy/flights
        4. Wait for window.appData.searchResult
        5. Parse journeyPairs → FlightOffers
        """
        t0 = time.monotonic()

        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
        )

        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            logger.info("easyJet: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.easyjet.com/en/",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.0)

            # Dismiss cookie/consent banners (may need to run twice as banner loads async)
            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            # Fill the search form
            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("easyJet: form fill failed, aborting")
                return self._empty(req)

            # Click "Show flights"
            try:
                await page.get_by_role("button", name="Show flights").click(timeout=5000)
                logger.info("easyJet: clicked 'Show flights', waiting for navigation")
            except Exception as e:
                logger.warning("easyJet: could not click 'Show flights': %s", e)
                return self._empty(req)

            # Wait for navigation to /buy/flights
            try:
                await page.wait_for_url("**/buy/flights**", timeout=15000)
                logger.info("easyJet: navigated to %s", page.url)
            except Exception:
                logger.warning("easyJet: didn't navigate to /buy/flights, current URL: %s", page.url)

            # Wait for appData
            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await page.wait_for_function(
                    "() => window.appData && window.appData.searchResult "
                    "&& window.appData.searchResult.journeyPairs",
                    timeout=int(remaining * 1000),
                )
            except Exception:
                logger.warning("easyJet: timed out waiting for searchResult after %.1fs (URL: %s)",
                              time.monotonic() - t0, page.url)
                return self._empty(req)

            data = await page.evaluate("""() => {
                const sr = window.appData.searchResult;
                if (!sr || !sr.journeyPairs) return null;
                return { journeyPairs: sr.journeyPairs, metaData: sr.metaData };
            }""")

            if not data or not data.get("journeyPairs"):
                logger.warning("easyJet: no journeyPairs in response")
                return self._empty(req)

            currency = data.get("metaData", {}).get("currencyCode", "GBP")
            offers = self._parse_journey_pairs(data["journeyPairs"], req, currency)

            elapsed = time.monotonic() - t0
            offers.sort(key=lambda o: o.price)

            logger.info(
                "easyJet %s→%s returned %d offers in %.1fs (Playwright)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash = hashlib.md5(
                f"easyjet{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=currency,
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("easyJet Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_journey_pairs(
        self, journey_pairs: list, req: FlightSearchRequest, currency: str
    ) -> list[FlightOffer]:
        """Parse journeyPairs from window.appData.searchResult into FlightOffers."""
        offers: list[FlightOffer] = []
        target_date = req.date_from.strftime("%Y-%m-%d")
        booking_url = self._build_booking_url(req)

        for pair in journey_pairs:
            outbound = pair.get("outbound", {})
            flights_by_date = outbound.get("flights", {})

            # flights is a dict keyed by date string
            for date_key, flight_list in flights_by_date.items():
                # Only include flights on the requested date (±1 day window may be returned)
                if date_key != target_date:
                    continue

                for flight in flight_list:
                    offer = self._parse_single_flight(flight, currency, booking_url)
                    if offer:
                        offers.append(offer)

        return offers

    def _parse_single_flight(
        self, flight: dict, currency: str, booking_url: str
    ) -> Optional[FlightOffer]:
        """Parse a single easyJet flight dict into a FlightOffer."""
        if flight.get("soldOut") or flight.get("saleableStatus") != "AVAILABLE":
            return None

        # Extract cheapest fare price
        fares = flight.get("fares", {})
        adt_fares = fares.get("ADT", {})
        price = None
        for fare_family in ["STANDARD", "FLEXI"]:
            fare = adt_fares.get(fare_family)
            if fare:
                unit_price = fare.get("unitPrice", {})
                gross = unit_price.get("grossPrice")
                if gross is not None:
                    if price is None or gross < price:
                        price = gross
                    break

        if price is None or price <= 0:
            return None

        flight_no = flight.get("flightNumber", "")
        carrier = flight.get("iataCarrierCode", "U2")
        if flight_no and not flight_no.startswith(carrier):
            flight_no = f"{carrier}{flight_no}"

        dep_str = flight.get("localDepartureDateTime", "")
        arr_str = flight.get("localArrivalDateTime", "")

        segment = FlightSegment(
            airline=carrier,
            airline_name="easyJet",
            flight_no=flight_no,
            origin=flight.get("departureAirportCode", ""),
            destination=flight.get("arrivalAirportCode", ""),
            departure=self._parse_dt(dep_str),
            arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

        total_dur = int((segment.arrival - segment.departure).total_seconds())

        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=max(total_dur, 0),
            stopovers=0,
        )

        key = f"{flight_no}_{dep_str}_{price}"

        return FlightOffer(
            id=f"ej_{hashlib.md5(key.encode()).hexdigest()[:12]}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["easyJet"],
            owner_airline="U2",
            booking_url=booking_url,
            is_locked=False,
            source="easyjet_direct",
            source_tier="free",
        )

    # ------------------------------------------------------------------
    # Form interaction (selectors verified Mar 2026)
    # ------------------------------------------------------------------

    async def _dismiss_cookies(self, page) -> None:
        """Remove ensighten / cookie banners that block clicks."""
        # Try clicking accept/agree buttons first
        for label in ["Accept", "Accept all", "Accept All Cookies", "I agree", "Got it", "OK"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{label}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    logger.info("easyJet: clicked cookie accept button '%s'", label)
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                continue

        # Force-remove overlay elements via JS
        try:
            await page.evaluate("""() => {
                const ids = ['ensBannerBG', 'ensNotifyBanner', 'onetrust-consent-sdk',
                              'ensCloseBanner', 'ens-banner-overlay'];
                ids.forEach(id => { const el = document.getElementById(id); if (el) el.remove(); });
                document.querySelectorAll(
                    '.ens-banner, [class*="cookie-banner"], [class*="consent"], ' +
                    '[class*="CookieBanner"], [id*="cookie"], [id*="consent"], ' +
                    '[class*="overlay"][style*="z-index"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
            }""")
        except Exception:
            pass
        await asyncio.sleep(0.3)

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        """Fill the easyJet homepage search form. Returns True on success.

        Form structure (verified Mar 2026):
        - textbox "From" with placeholder "Country, city, airport"
        - textbox "To" with placeholder "Country, city, airport"
        - textbox "Clear selected travel date" (calendar trigger)
        - button "Show flights"
        """
        # Fill 'From' airport
        ok = await self._fill_airport_field(page, "From", req.origin)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        # Fill 'To' airport
        ok = await self._fill_airport_field(page, "To", req.destination)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        # Fill date
        ok = await self._fill_date(page, req)
        if not ok:
            return False
        return True

    async def _fill_airport_field(self, page, label: str, iata: str) -> bool:
        """Fill an airport textbox and select the matching suggestion.

        The easyJet form renders textbox "From" / textbox "To".
        Typing the IATA code triggers an autocomplete dropdown with
        radio buttons like ``radio "London Gatwick ( LGW ) United Kingdom"``.
        Returns True on success.
        """
        try:
            field = page.get_by_role("textbox", name=label)
            # Clear existing value
            if label == "From":
                clear_name = "Clear selected departure airport"
            else:
                clear_name = "Clear selected destination airport"
            try:
                clear_btn = page.get_by_role("button", name=clear_name)
                if await clear_btn.count() > 0:
                    await clear_btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    logger.info("easyJet: cleared %s field", label)
            except Exception:
                pass

            await field.click(timeout=3000)
            await asyncio.sleep(0.3)
            await field.fill(iata)
            logger.info("easyJet: typed '%s' in %s field, waiting for suggestions", iata, label)
            await asyncio.sleep(2.0)

            # Suggestions appear as radio buttons: "London Gatwick ( LGW ) United Kingdom"
            # Note: spaces around the IATA code inside parentheses
            option = page.get_by_role("radio", name=re.compile(
                rf"{re.escape(iata)}", re.IGNORECASE
            )).first
            await option.click(timeout=5000)
            logger.info("easyJet: selected %s airport for %s", iata, label)
            return True
        except Exception as e:
            logger.warning("easyJet: %s field error: %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Open the date picker and select the outbound date.

        The calendar has:
        - Day buttons labeled "April 15, 2026" (US format with comma)
        - Month headings "APRIL 2026" (uppercase)
        - Navigation: button "Previous month" / button "Next month"
        - Toggle: button "Return trip" → click to switch to one-way
        
        Flow: open calendar → navigate to month → click outbound day → handle return.
        Returns True on success.
        """
        target = req.date_from

        try:
            # Click the date field to open the calendar
            try:
                date_field = page.get_by_role("textbox", name="Clear selected travel date")
                if await date_field.count() == 0:
                    date_field = page.get_by_placeholder("Choose your dates")
                await date_field.click(timeout=3000)
            except Exception:
                when_section = page.locator("text=When").first
                await when_section.click(timeout=3000)
            await asyncio.sleep(0.8)
            logger.info("easyJet: opened date picker")

            # Navigate to the right month FIRST (before any toggling).
            month_upper = target.strftime("%B %Y").upper()  # e.g. "APRIL 2026"
            for i in range(12):
                heading = page.get_by_role("heading", name=month_upper)
                if await heading.count() > 0:
                    logger.info("easyJet: found month heading '%s'", month_upper)
                    break
                try:
                    fwd = page.get_by_role("button", name="Next month")
                    await fwd.click(timeout=2000)
                    await asyncio.sleep(0.4)
                except Exception as e:
                    logger.warning("easyJet: could not click 'Next month': %s", e)
                    break

            # Click the outbound day button.
            day_label = f"{target.strftime('%B')} {target.day}, {target.year}"
            day_btn = page.get_by_role("button", name=day_label)
            await day_btn.click(timeout=5000)
            logger.info("easyJet: clicked outbound date %s", day_label)
            await asyncio.sleep(0.5)

            # After selecting outbound, calendar asks for return date.
            # Either switch to one-way or pick a return date +7 days.
            try:
                return_btn = page.get_by_role("button", name="Return trip")
                if await return_btn.count() > 0:
                    await return_btn.click(timeout=2000)
                    await asyncio.sleep(0.3)
                    logger.info("easyJet: switched to one-way")
                else:
                    # Already one-way or no toggle visible — pick return date
                    from datetime import timedelta
                    ret = target + timedelta(days=7)
                    ret_label = f"{ret.strftime('%B')} {ret.day}, {ret.year}"
                    ret_btn = page.get_by_role("button", name=ret_label)
                    if await ret_btn.count() > 0:
                        await ret_btn.click(timeout=2000)
                        logger.info("easyJet: picked return date %s", ret_label)
            except Exception:
                pass

            await asyncio.sleep(0.5)
            return True
        except Exception as e:
            logger.warning("easyJet: Date error: %s", e)
            return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        date_out = req.date_from.strftime("%Y-%m-%d")
        url = (
            f"https://www.easyjet.com/en/buy/flights"
            f"?dep={req.origin}&dest={req.destination}"
            f"&dd={date_out}&isOneWay=on"
            f"&apax={req.adults}&cpax={req.children or 0}"
            f"&ipax={req.infants or 0}"
        )
        return url

    def _parse_dt(self, s: str) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"easyjet{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
