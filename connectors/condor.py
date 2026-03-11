"""
Condor Playwright connector — navigates to condor.com and searches flights.

Condor (IATA: DE) is a German leisure airline operating from Frankfurt (FRA),
Munich (MUC), Düsseldorf (DUS), Hamburg (HAM) and other German airports to
worldwide holiday destinations.

The direct API is behind WAF — requires browser session.

Strategy:
1. Navigate to condor.com/en/flights homepage
2. Dismiss cookie consent banner (Usercentrics — "I agree")
3. Fill search form (From, To, Outbound date, one-way selection)
4. Intercept API responses (search / availability / offers endpoints)
5. Parse results → FlightOffers

Condor search form (verified Mar 2026):
  - "Round Trip" dropdown (change to one-way)
  - "From" input (origin)
  - "To" input (destination)
  - "Outbound flight on" date picker
  - "Return flight on" date picker
  - "1 Passenger, Economy" passenger/class selector
  - "Search for flights" button
  Cookie banner: Usercentrics with "I agree" / "Reject optional Cookies" / "Settings"
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import re
import time
from datetime import datetime
from typing import Any, Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

# ── Anti-fingerprint pools ─────────────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
]
_LOCALES = ["en-GB", "en-US", "de-DE", "en-IE"]
_TIMEZONES = [
    "Europe/Berlin", "Europe/London",
    "Europe/Paris", "Europe/Vienna", "Europe/Zurich",
]

# ── Shared browser singleton ──────────────────────────────────────────────
_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Shared headed Chromium (launched once, reused across searches)."""
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
        logger.info("Condor: Playwright browser launched (headed Chrome)")
        return _browser


class CondorConnectorClient:
    """Condor Playwright connector — homepage form search + API interception."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
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

            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    ct = response.headers.get("content-type", "")
                    if response.status != 200 or "json" not in ct:
                        return
                    # Primary: Condor TCA vacancies endpoint (NOT lowFareInformation)
                    is_vacancies = (
                        "/vacancies" in url
                        and "lowfare" not in url
                        and "tca/rest" in url
                    )
                    # Secondary: generic flight search APIs (for other possible endpoints)
                    is_generic = (
                        "availability" in url
                        or "flights/search" in url
                        or "air-bounds" in url
                    )
                    if is_vacancies or is_generic:
                        data = await response.json()
                        if data and isinstance(data, (dict, list)):
                            captured_data["json"] = data
                            api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("Condor: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://www.condor.com/en/flights",
                wait_until="domcontentloaded",
                timeout=int(self.timeout * 1000),
            )
            await asyncio.sleep(2.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("Condor: form fill failed")
                return self._empty(req)

            await self._click_search(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Condor: timed out waiting for API response")
                offers = await self._extract_from_dom(page, req)
                if offers:
                    return self._build_response(offers, req, time.monotonic() - t0)
                return self._empty(req)

            data = captured_data.get("json", {})
            if not data:
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_response(data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Condor Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ── Cookie dismissal ───────────────────────────────────────────────

    async def _dismiss_cookies(self, page) -> None:
        # Condor uses Usercentrics: "I agree" / "Settings" / "Reject optional Cookies"
        for label in [
            "I agree", "Agree", "Accept all",
            "Accept All", "ACCEPT ALL", "Accept",
            "Accept all cookies",
        ]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue

        # Usercentrics may use shadow DOM — try inside iframes too
        try:
            for frame in page.frames:
                for label in ["I agree", "Agree", "Accept all"]:
                    try:
                        btn = frame.get_by_role(
                            "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                        )
                        if await btn.count() > 0:
                            await btn.first.click(timeout=2000)
                            await asyncio.sleep(0.5)
                            return
                    except Exception:
                        continue
        except Exception:
            pass

        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], ' +
                    '[class*="Cookie"], [id*="Cookie"], [class*="onetrust"], [id*="onetrust"], ' +
                    '[class*="uc-"], [id*="usercentrics"], [class*="usercentrics"], ' +
                    '#uc-center-container'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── Form filling ───────────────────────────────────────────────────

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        # Set one-way (Condor defaults to "Round Trip")
        await self._set_one_way(page)
        await asyncio.sleep(0.3)

        # Fill origin (Condor labels: "From" / "Input Origin")
        ok = await self._fill_airport_field(page, "From", req.origin, 0)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        # Fill destination (Condor labels: "To" / "Input destination")
        ok = await self._fill_airport_field(page, "To", req.destination, 1)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        # Set date (Condor labels: "Outbound flight on" / "Select dates")
        ok = await self._fill_date(page, req)
        return ok

    async def _fill_airport_field(self, page, label: str, iata: str, index: int) -> bool:
        # Condor comboboxes: "fc-booking-origin-aria-label" / "fc-booking-destination-aria-label"
        aria_labels = {
            "From": "fc-booking-origin-aria-label",
            "To": "fc-booking-destination-aria-label",
        }
        aria = aria_labels.get(label, "")

        try:
            field = None
            # Try specific Condor aria-label first
            if aria:
                f = page.get_by_role("combobox", name=aria)
                if await f.count() > 0:
                    field = f.first
            # Fallback: generic label match
            if field is None:
                for role in ["combobox", "textbox"]:
                    f = page.get_by_role(role, name=re.compile(rf"{label}", re.IGNORECASE))
                    if await f.count() > 0:
                        field = f.first
                        break

            if field is not None:
                await field.click(timeout=3000)
                await asyncio.sleep(0.3)
                await field.fill("")
                await asyncio.sleep(0.2)
                await field.fill(iata)
                await asyncio.sleep(2.0)

                # Condor suggestions are <DIV role="option"> containing IATA code
                option = page.get_by_role("option").filter(
                    has_text=re.compile(rf"{re.escape(iata)}", re.IGNORECASE)
                ).first
                if await option.count() > 0:
                    await option.click(timeout=3000)
                    logger.info("Condor: selected %s for %s", iata, label)
                    return True

                # Fallback: press Enter to accept top suggestion
                await page.keyboard.press("Enter")
                return True
        except Exception as e:
            logger.debug("Condor: %s field error: %s", label, e)

        # Last resort: index-based input
        try:
            inputs = page.locator("input[type='text'], input[type='search']")
            if await inputs.count() > index:
                field = inputs.nth(index)
                await field.click(timeout=3000)
                await field.fill("")
                await asyncio.sleep(0.2)
                await field.fill(iata)
                await asyncio.sleep(2.0)
                await page.keyboard.press("Enter")
                return True
        except Exception:
            pass
        return False

    async def _set_one_way(self, page) -> None:
        # Condor: click the "Round Trip" button to open dropdown, then pick "One-way" LI
        try:
            trip_btn = page.get_by_role(
                "button", name=re.compile(r"Round Trip", re.IGNORECASE)
            )
            if await trip_btn.count() > 0:
                await trip_btn.first.click(timeout=3000)
                await asyncio.sleep(0.5)
                ow = page.get_by_text("One-way", exact=False).first
                if await ow.count() > 0:
                    await ow.click(timeout=2000)
                    logger.info("Condor: switched to One-way")
                    return
        except Exception as e:
            logger.debug("Condor: one-way via Round Trip button failed: %s", e)

        # Fallback: try radio button
        try:
            radio = page.get_by_role("radio", name=re.compile(r"one.?way", re.IGNORECASE))
            if await radio.count() > 0:
                await radio.first.click(timeout=2000)
                return
        except Exception:
            pass

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        # Condor date buttons have aria-label="MM/DD/YYYY, , " format
        target_label = target.strftime("%m/%d/%Y")
        try:
            # Click the departure date button to open calendar
            date_btn = page.get_by_role(
                "button", name=re.compile(r"fc-booking-departure-date-aria-label", re.IGNORECASE)
            )
            if await date_btn.count() > 0:
                await date_btn.first.click(timeout=3000)
            else:
                # Fallback: click the "Outbound flight on" area or any date-related element
                for sel in ["Outbound flight", "Departure", "Date", "MM/DD/YYYY"]:
                    el = page.get_by_text(sel, exact=False).first
                    if await el.count() > 0:
                        await el.click(timeout=3000)
                        break
            await asyncio.sleep(0.8)

            # Navigate months forward until the target day button is visible
            # Condor calendar shows 2 months side-by-side
            for _ in range(12):
                day_btn = page.locator(
                    f"button[role='gridcell'][aria-label^='{target_label},']"
                )
                if await day_btn.count() > 0:
                    is_disabled = await day_btn.first.get_attribute("disabled")
                    aria_disabled = await day_btn.first.get_attribute("aria-disabled")
                    if is_disabled == "true" or aria_disabled == "true":
                        logger.warning("Condor: target date %s is disabled", target_label)
                        return False
                    await day_btn.first.click(timeout=3000)
                    logger.info("Condor: selected date %s", target_label)
                    await asyncio.sleep(0.5)
                    # Click "Done" if present
                    try:
                        done_btn = page.get_by_role("button", name="Done")
                        if await done_btn.count() > 0:
                            await done_btn.first.click(timeout=2000)
                            await asyncio.sleep(0.3)
                    except Exception:
                        pass
                    return True

                # Target not visible yet — click next month arrow
                fwd = page.locator(
                    "button:has-text('keyboard_arrow_right')"
                ).first
                if await fwd.count() > 0:
                    await fwd.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    continue
                # Fallback: try right-arrow positioned button
                fwd2 = page.locator(
                    "button[class*='right-6'], button[class*='arrow-right']"
                ).first
                if await fwd2.count() > 0:
                    await fwd2.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    continue
                logger.warning("Condor: no next-month button found")
                break

            logger.warning("Condor: date %s not found in calendar", target_label)
            return False
        except Exception as e:
            logger.warning("Condor: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        for label in ["Search for flights", "Search flights", "Search", "SEARCH", "Find flights"]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    logger.info("Condor: clicked search")
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    # ── DOM fallback ───────────────────────────────────────────────────

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                if (window.appData) return window.appData;
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try {
                        const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.outbound || d.journeys || d.fares)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
        return []

    # ── Response parsing ───────────────────────────────────────────────

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Condor TCA API format: {data: [[{segment1, vacancyDetails: [...]}], ...], messages: [...]}
        flights_list = None
        if isinstance(data, dict) and "data" in data:
            raw = data["data"]
            if isinstance(raw, list) and raw:
                # data[0] = outbound flights array, data[1] = inbound (empty for one-way)
                if isinstance(raw[0], list):
                    flights_list = raw[0]
                else:
                    flights_list = raw
        elif isinstance(data, list):
            # Might be the raw array directly
            if data and isinstance(data[0], list):
                flights_list = data[0]
            else:
                flights_list = data

        if not flights_list:
            return offers

        for flight in flights_list:
            if not isinstance(flight, dict):
                continue
            parsed = self._parse_tca_flight(flight, req, booking_url)
            if parsed:
                offers.extend(parsed)

        return offers

    def _parse_tca_flight(
        self, flight: dict, req: FlightSearchRequest, booking_url: str,
    ) -> list[FlightOffer]:
        """Parse a single Condor TCA flight with multiple fare bundles."""
        offers: list[FlightOffer] = []
        vacancy_details = flight.get("vacancyDetails", [])
        if not vacancy_details:
            return offers

        # Build segments from legs (or the flight itself for direct)
        legs = flight.get("legs") or [flight]
        segments: list[FlightSegment] = []
        for leg in legs:
            dep_str = leg.get("departure", "")
            arr_str = leg.get("arrival", "")
            segments.append(FlightSegment(
                airline="DE",
                airline_name="Condor",
                flight_no=f"DE{leg.get('flightNumber', '')}",
                origin=leg.get("origin", req.origin),
                destination=leg.get("destination", req.destination),
                departure=self._parse_condor_dt(dep_str),
                arrival=self._parse_condor_dt(arr_str),
                cabin_class="M",
            ))

        total_dur = 0
        if segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

        # Create one offer per fare bundle (tariff)
        for vc in vacancy_details:
            price_details = vc.get("priceDetails", [])
            if not price_details:
                continue
            # Get GROSS_PRICE from first reduction
            components = price_details[0].get("components", [])
            gross = None
            currency = price_details[0].get("currency", req.currency or "EUR")
            for comp in components:
                if comp.get("type") == "GROSS_PRICE":
                    gross = comp.get("value")
                    break
            if gross is None:
                continue
            # Prices are in cents
            price = round(gross / 100.0, 2)
            if price <= 0:
                continue

            compartment = vc.get("compartment", "Y")
            tariff = vc.get("tariff", "")
            cabin_map = {"Y": "economy", "C": "business", "P": "premium_economy", "F": "first"}
            cabin = cabin_map.get(compartment, "economy")
            flight_key = f"DE{flight.get('flightNumber', '')}_{tariff}_{compartment}"

            offers.append(FlightOffer(
                id=f"de_{hashlib.md5(flight_key.encode()).hexdigest()[:12]}",
                price=price,
                currency=currency,
                price_formatted=f"{price:.2f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Condor"],
                owner_airline="DE",
                booking_url=booking_url,
                is_locked=False,
                source="condor_direct",
                source_tier="free",
            ))

        return offers

    # ── Helpers ────────────────────────────────────────────────────────

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Condor %s→%s returned %d offers in %.1fs (Playwright)", req.origin, req.destination, len(offers), elapsed)
        search_hash = hashlib.md5(f"condor{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=offers[0].currency if offers else (req.currency or "EUR"),
            offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _parse_condor_dt(s: Any) -> datetime:
        """Parse Condor TCA datetime format: 20260415T0715+0200."""
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        # Condor format: YYYYMMDDTHHMM+TZOFFSET (e.g. 20260415T0715+0200)
        m = re.match(r"(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})([+-]\d{4})?", s)
        if m:
            return datetime(
                int(m.group(1)), int(m.group(2)), int(m.group(3)),
                int(m.group(4)), int(m.group(5)),
            )
        # Fallback to standard ISO
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        return datetime(2000, 1, 1)

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.condor.com/en/flights?from={req.origin}"
            f"&to={req.destination}&departure={dep}"
            f"&adults={req.adults}&children={req.children}&infants={req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"condor{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}", origin=req.origin, destination=req.destination,
            currency=req.currency or "EUR", offers=[], total_results=0,
        )
