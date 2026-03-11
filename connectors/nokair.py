"""
Nok Air Playwright connector — booking.nokair.com direct navigation.

Nok Air (IATA: DD) is a Thai low-cost carrier based at Don Mueang (DMK).
Booking: booking.nokair.com/en (Vue.js SPA, Navitaire backend).

Strategy (verified Mar 2026):
1. Navigate DIRECTLY to booking.nokair.com/en (NOT www.nokair.com — form is
   inside an iframe there, and banner modal blocks clicks)
2. Click "One Way" trip type button (#search-type-oneway)
3. Fill origin: searchbox "Select Origin" → type IATA → click matching option
4. Fill destination: searchbox "Select Destination" → type IATA → click option
5. Open datepicker, navigate months via "next month" button, click day button
   (format: "MonthName Day", e.g. "April 15")
6. Click "Search" → page navigates to results
7. Intercept Navitaire availability API responses → parse into FlightOffers

Form is Vue.js (vue-select comboboxes). Options have names like "FromDon Mueang
(Bangkok)" with IATA in parentheses.
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

_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
    {"width": 1600, "height": 900},
]
_LOCALES = ["en-US", "en-GB", "en-TH", "th-TH"]
_TIMEZONES = [
    "Asia/Bangkok", "Asia/Singapore", "Asia/Tokyo",
    "Asia/Kolkata", "Europe/London",
]

_pw_instance = None
_browser = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    global _pw_instance, _browser
    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright
        _pw_instance = await async_playwright().start()
        try:
            _browser = await _pw_instance.chromium.launch(
                headless=False, channel="chrome",
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
        logger.info("NokAir: Playwright browser launched (headed Chrome)")
        return _browser


class NokAirConnectorClient:
    """Nok Air Playwright connector — homepage form search + API interception."""

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
        )
        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            captured_data: list[dict] = []

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status == 200 and (
                        "ezycommerce" in url or "availability" in url
                        or "/api/nsk" in url or "searchshop" in url
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, (dict, list)):
                                captured_data.append(data)
                                logger.info("NokAir: captured API response from %s", response.url[:120])
                except Exception:
                    pass

            page.on("response", on_response)

            # Navigate directly to the booking page (NOT www.nokair.com)
            logger.info("NokAir: loading booking page for %s→%s", req.origin, req.destination)
            await page.goto("https://booking.nokair.com/en",
                            wait_until="load", timeout=int(self.timeout * 1000))
            logger.info("NokAir: page loaded, URL=%s", page.url)

            # Wait for Vue.js form to hydrate (Search button appears)
            await asyncio.sleep(2.0)
            try:
                await page.get_by_role("button", name="Search", exact=True).wait_for(state="visible", timeout=20000)
            except Exception as e:
                logger.warning("NokAir: form did not load in time: %s", e)
                return self._empty(req)
            await asyncio.sleep(1.0)

            # Click One Way
            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            # Fill origin
            ok = await self._fill_airport(page, req.origin, is_origin=True)
            if not ok:
                logger.warning("NokAir: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill destination
            ok = await self._fill_airport(page, req.destination, is_origin=False)
            if not ok:
                logger.warning("NokAir: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            # Fill date
            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("NokAir: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)

            # Click Search — triggers navigation to /en/select
            await self._click_search(page)

            # Wait for results page to load (URL changes to /select)
            try:
                await page.wait_for_url("**/select**", timeout=20000)
            except Exception:
                logger.warning("NokAir: did not navigate to results page")
                return self._empty(req)
            await asyncio.sleep(2.0)

            # Primary: Extract flights from the rendered DOM
            offers = await self._extract_from_dom(page, req)

            # Bonus: if API data was captured, try parsing it too
            if not offers and captured_data:
                for data in captured_data:
                    parsed = self._parse_response(data, req)
                    if parsed:
                        offers.extend(parsed)
                    offers.extend(parsed)
            if not offers:
                # Fallback: try DOM extraction
                offers = await self._extract_from_dom(page, req)

            elapsed = time.monotonic() - t0
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("NokAir Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    async def _set_one_way(self, page) -> None:
        """Click the One Way trip type button."""
        try:
            btn = page.get_by_role("button", name="oneway search button")
            if await btn.count() > 0:
                await btn.first.click(timeout=5000)
                logger.info("NokAir: selected One Way")
                return
        except Exception:
            pass
        # Fallback: click by ID
        try:
            await page.locator("#search-type-oneway").click(timeout=3000)
        except Exception as e:
            logger.debug("NokAir: one-way click error: %s", e)

    async def _fill_airport(self, page, iata: str, is_origin: bool) -> bool:
        """Fill airport using vue-select searchbox (type IATA code, click option)."""
        label = "Select Origin" if is_origin else "Select Destination"
        try:
            # Click the searchbox to open the dropdown
            searchbox = page.get_by_role("searchbox", name=label)
            if await searchbox.count() == 0:
                logger.warning("NokAir: searchbox '%s' not found", label)
                return False

            await searchbox.click(timeout=5000)
            await asyncio.sleep(0.5)

            # Type IATA code character by character (triggers Vue.js events)
            await searchbox.fill("")
            await asyncio.sleep(0.2)
            await searchbox.press_sequentially(iata, delay=100)
            await asyncio.sleep(2.0)

            # Wait for filtered option containing the IATA code
            option = page.get_by_role("option").filter(has_text=f"({iata})")
            try:
                await option.first.wait_for(state="visible", timeout=5000)
                await option.first.click(timeout=3000)
                logger.info("NokAir: selected %s for %s", iata, "origin" if is_origin else "destination")
                return True
            except Exception:
                pass

            # Fallback: look through all options by text_content
            options = page.get_by_role("option")
            count = await options.count()
            logger.debug("NokAir: found %d options for '%s'", count, iata)
            for i in range(count):
                opt = options.nth(i)
                text = await opt.text_content() or ""
                if f"({iata})" in text or iata.lower() in text.lower():
                    await opt.click(timeout=3000)
                    logger.info("NokAir: selected %s via text scan", iata)
                    return True

            # Last resort: press Enter to accept whatever is highlighted
            await page.keyboard.press("Enter")
            logger.info("NokAir: pressed Enter for %s", iata)
            return True

        except Exception as e:
            logger.debug("NokAir: airport fill error (%s): %s", label, e)
            return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Open datepicker, navigate to target month, click the day button.

        NokAir datepicker uses:
        - button "open datepicker" to open
        - heading showing "MonthName YYYY" (e.g. "April 2026")
        - button "next month" / "previous month" for navigation
        - day buttons named "MonthName Day" (e.g. "April 15")
        """
        target = req.date_from
        target_month_year = target.strftime("%B %Y")  # e.g. "April 2026"
        day_label = f"{target.strftime('%B')} {target.day}"  # e.g. "April 15"

        try:
            # Open the datepicker
            dp_btn = page.get_by_role("button", name="open datepicker")
            if await dp_btn.count() > 0:
                await dp_btn.first.click(timeout=5000)
            else:
                # Fallback: click any date-related control
                dp_alt = page.locator("[class*='date'], [id*='date']").first
                if await dp_alt.count() > 0:
                    await dp_alt.click(timeout=3000)
            await asyncio.sleep(0.8)

            # Navigate to the target month (up to 18 months forward)
            for _ in range(18):
                content = await page.content()
                if target_month_year.lower() in content.lower():
                    break
                next_btn = page.get_by_role("button", name="next month")
                if await next_btn.count() > 0:
                    await next_btn.first.click(timeout=2000)
                    await asyncio.sleep(0.4)
                else:
                    logger.warning("NokAir: 'next month' button not found")
                    break

            # Click the day button (named "MonthName Day")
            day_btn = page.get_by_role("button", name=day_label)
            if await day_btn.count() > 0:
                await day_btn.first.click(timeout=3000)
                logger.info("NokAir: selected date %s", day_label)
                return True

            # Fallback: try button with just the day number
            day_btn = page.get_by_role("button", name=re.compile(rf"^{target.day}$"))
            if await day_btn.count() > 0:
                await day_btn.first.click(timeout=3000)
                return True

            logger.warning("NokAir: day button '%s' not found", day_label)
            return False
        except Exception as e:
            logger.warning("NokAir: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        """Click the Search button to submit the form."""
        try:
            btn = page.get_by_role("button", name="Search", exact=True)
            if await btn.count() > 0:
                await btn.first.click(timeout=5000)
                return
        except Exception:
            pass
        # Fallback: use the button ID
        try:
            await page.locator("#criteria-search-button").click(timeout=5000)
        except Exception:
            await page.keyboard.press("Enter")

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from the NokAir results page DOM.

        The /en/select page renders flights as article elements:
        - article "Flight" → contains application with aria-label describing the flight
        - Each flight has fare buttons: "NOK LITE. 1,000.00", "NOK XTRA. 1,550.00", etc.
        - Flight info includes departure/arrival times, flight number, duration
        """
        try:
            # Wait for flight articles to appear
            articles = page.locator("article")
            try:
                await articles.first.wait_for(state="visible", timeout=10000)
            except Exception:
                logger.warning("NokAir: no flight articles found on results page")
                return []

            offers: list[FlightOffer] = []
            count = await articles.count()
            booking_url = self._build_booking_url(req)

            for i in range(count):
                try:
                    article = articles.nth(i)
                    # The application element has the flight details in its accessible name
                    # Format: "From {Origin} To {Dest} {Day} {Date}, {DepTime} - {Day} {Date}, {ArrTime}. undefined."
                    app = article.locator("[role='application']")
                    if await app.count() == 0:
                        continue

                    aria_label = await app.first.get_attribute("aria-label") or ""

                    # Extract times from aria-label
                    # e.g. "...Wednesday 15 April 2026, 06:30 - Wednesday 15 April 2026, 07:45..."
                    time_match = re.search(r"(\d{2}:\d{2})\s*-\s*\w+\s+\d+\s+\w+\s+\d{4},\s*(\d{2}:\d{2})", aria_label)
                    dep_time = time_match.group(1) if time_match else ""
                    arr_time = time_match.group(2) if time_match else ""

                    # Extract flight number (DD xxx) from the article text
                    flight_text = await article.inner_text()
                    fno_match = re.search(r"(DD\s*\d+)", flight_text)
                    flight_no = fno_match.group(1).replace(" ", "") if fno_match else ""

                    # Extract duration
                    dur_match = re.search(r"(\d+)h\s*(\d+)m", flight_text)
                    dur_seconds = 0
                    if dur_match:
                        dur_seconds = int(dur_match.group(1)) * 3600 + int(dur_match.group(2)) * 60

                    # Build departure/arrival datetimes
                    dep_dt = self._parse_dt(f"{req.date_from.isoformat()}T{dep_time}:00") if dep_time else datetime(2000, 1, 1)
                    arr_dt = self._parse_dt(f"{req.date_from.isoformat()}T{arr_time}:00") if arr_time else datetime(2000, 1, 1)

                    # Extract lowest fare price from buttons
                    # Fare buttons have text like "NOK LITE. 1,000.00", "NOK XTRA. 1,550.00"
                    best_price = float("inf")
                    price_matches = re.findall(r"[\d,]+\.\d{2}", flight_text)
                    for pm in price_matches:
                        try:
                            val = float(pm.replace(",", ""))
                            if 0 < val < best_price:
                                best_price = val
                        except (ValueError, TypeError):
                            pass

                    if best_price == float("inf") or best_price <= 0:
                        continue

                    segment = FlightSegment(
                        airline="DD", airline_name="Nok Air", flight_no=flight_no,
                        origin=req.origin, destination=req.destination,
                        departure=dep_dt, arrival=arr_dt, cabin_class="M",
                    )
                    route = FlightRoute(
                        segments=[segment],
                        total_duration_seconds=dur_seconds or max(int((arr_dt - dep_dt).total_seconds()), 0),
                        stopovers=0,
                    )
                    offer_id = f"dd_{hashlib.md5(f'{flight_no}_{dep_time}'.encode()).hexdigest()[:12]}"
                    offers.append(FlightOffer(
                        id=offer_id, price=round(best_price, 2), currency=req.currency,
                        price_formatted=f"{best_price:.2f} {req.currency}",
                        outbound=route, inbound=None, airlines=["Nok Air"], owner_airline="DD",
                        booking_url=booking_url, is_locked=False, source="nokair_direct", source_tier="free",
                    ))
                except Exception as e:
                    logger.debug("NokAir: error parsing flight article %d: %s", i, e)
                    continue

            return offers

        except Exception as e:
            logger.warning("NokAir: DOM extraction error: %s", e)
            return []

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        flights_raw = (
            data.get("outboundFlights") or data.get("outbound") or data.get("journeys")
            or data.get("flights") or data.get("availability", {}).get("trips", [])
            or data.get("data", {}).get("flights", []) or data.get("data", {}).get("journeys", [])
            or data.get("lowFareAvailability", {}).get("outboundOptions", [])
            or data.get("flightList", []) or data.get("tripOptions", []) or []
        )
        if isinstance(flights_raw, dict):
            flights_raw = flights_raw.get("outbound", []) or flights_raw.get("journeys", [])
        if not isinstance(flights_raw, list):
            flights_raw = []
        for flight in flights_raw:
            offer = self._parse_single_flight(flight, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    def _parse_single_flight(self, flight: dict, req: FlightSearchRequest, booking_url: str) -> Optional[FlightOffer]:
        best_price = self._extract_best_price(flight)
        if best_price is None or best_price <= 0:
            return None
        segments_raw = flight.get("segments") or flight.get("legs") or flight.get("flights") or []
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination))
        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())
        route = FlightRoute(segments=segments, total_duration_seconds=max(total_dur, 0), stopovers=max(len(segments) - 1, 0))
        flight_key = flight.get("journeyKey") or flight.get("id") or f"{flight.get('departureDate', '')}_{time.monotonic()}"
        return FlightOffer(
            id=f"dd_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=req.currency,
            price_formatted=f"{best_price:.2f} {req.currency}",
            outbound=route, inbound=None, airlines=["Nok Air"], owner_airline="DD",
            booking_url=booking_url, is_locked=False, source="nokair_direct", source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        fares = flight.get("fares") or flight.get("fareProducts") or flight.get("bundles") or flight.get("fareBundles") or []
        best = float("inf")
        for fare in fares:
            if isinstance(fare, dict):
                for key in ["price", "amount", "totalPrice", "basePrice", "fareAmount", "totalAmount"]:
                    val = fare.get(key)
                    if isinstance(val, dict):
                        val = val.get("amount") or val.get("value")
                    if val is not None:
                        try:
                            v = float(val)
                            if 0 < v < best:
                                best = v
                        except (TypeError, ValueError):
                            pass
        for key in ["price", "lowestFare", "totalPrice", "farePrice", "amount", "lowestPrice"]:
            p = flight.get(key)
            if p is not None:
                try:
                    v = float(p) if not isinstance(p, dict) else float(p.get("amount", 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass
        return best if best < float("inf") else None

    def _build_segment(self, seg: dict, default_origin: str, default_dest: str) -> FlightSegment:
        dep_str = seg.get("departureDateTime") or seg.get("departure") or seg.get("departureDate") or seg.get("std") or ""
        arr_str = seg.get("arrivalDateTime") or seg.get("arrival") or seg.get("arrivalDate") or seg.get("sta") or ""
        flight_no = str(seg.get("flightNumber") or seg.get("flight_no") or seg.get("number") or "").replace(" ", "")
        origin = seg.get("origin") or seg.get("departureStation") or seg.get("departureAirport") or default_origin
        destination = seg.get("destination") or seg.get("arrivalStation") or seg.get("arrivalAirport") or default_dest
        carrier = seg.get("carrierCode") or seg.get("carrier") or seg.get("airline") or "DD"
        return FlightSegment(
            airline=carrier, airline_name="Nok Air", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str), cabin_class="M",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("NokAir %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"nokair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _parse_dt(s: Any) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        s = str(s)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://booking.nokair.com/en/search?origin={req.origin}"
            f"&destination={req.destination}&departure={dep}&adults={req.adults}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"nokair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
