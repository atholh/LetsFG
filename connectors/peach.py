"""
Peach Aviation Playwright connector — direct booking URL approach.

Peach Aviation (IATA: MM) is a Japanese LCC (ANA group).
Booking site: booking.flypeach.com

Strategy:
1. Build direct search URL with JSON params (bypasses homepage form entirely)
2. Navigate to booking.flypeach.com/en/getsearch?s=[params]
3. Click through the confirmation page
4. Extract flight data from server-rendered DOM
5. Parse → FlightOffer objects
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
import time
import urllib.parse
from datetime import datetime, timedelta
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
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
]
_LOCALES = ["en-US", "en-GB", "en-JP"]
_TIMEZONES = ["Asia/Tokyo", "Asia/Seoul", "Asia/Shanghai", "Asia/Taipei"]

_pw_instance = None
_browser = None
_persistent_context = None
_browser_lock: Optional[asyncio.Lock] = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    global _pw_instance, _browser, _persistent_context
    lock = _get_lock()
    async with lock:
        if _persistent_context and _persistent_context.browser and _persistent_context.browser.is_connected():
            return _persistent_context
        if _browser and _browser.is_connected():
            return _browser
        from playwright.async_api import async_playwright
        _pw_instance = await async_playwright().start()
        args = [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
        ]
        # Try persistent context first (preserves cookies/reCAPTCHA score)
        import tempfile, os
        user_data = os.path.join(tempfile.gettempdir(), "peach_browser_data")
        os.makedirs(user_data, exist_ok=True)
        try:
            _persistent_context = await _pw_instance.chromium.launch_persistent_context(
                user_data_dir=user_data,
                headless=False, channel="chrome", args=args,
                viewport={"width": 1440, "height": 900},
                locale="en-US",
                timezone_id="Asia/Tokyo",
                service_workers="block",
            )
            logger.info("Peach: persistent browser context launched (Chrome)")
            return _persistent_context
        except Exception:
            try:
                _browser = await _pw_instance.chromium.launch(
                    headless=False, channel="chrome", args=args,
                )
            except Exception:
                _browser = await _pw_instance.chromium.launch(
                    headless=False, args=args,
                )
            logger.info("Peach: Playwright browser launched (headed Chrome, non-persistent)")
            return _browser


class PeachConnectorClient:
    """Peach Aviation connector — direct booking URL + DOM extraction."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser_or_ctx = await _get_browser()

        # Persistent context IS the context; regular browser needs a new context
        is_persistent = hasattr(browser_or_ctx, 'new_page') and not hasattr(browser_or_ctx, 'new_context')
        if is_persistent:
            context = browser_or_ctx
            own_context = False
        else:
            context = await browser_or_ctx.new_context(
                viewport=random.choice(_VIEWPORTS),
                locale=random.choice(_LOCALES),
                timezone_id=random.choice(_TIMEZONES),
                service_workers="block",
                color_scheme=random.choice(["light", "dark", "no-preference"]),
            )
            own_context = True
        try:
            try:
                from playwright_stealth import Stealth
                page = await context.new_page()
                await Stealth().apply_stealth_async(page)
                logger.info("Peach: stealth applied to page")
            except Exception:
                page = await context.new_page()
                logger.info("Peach: stealth not available, using plain page")

            search_url = self._build_search_url(req)
            logger.info("Peach: navigating to booking URL for %s→%s on %s",
                        req.origin, req.destination, req.date_from.strftime("%Y/%m/%d"))

            # Step 1: Navigate to getsearch URL to set session data (origin/dest/date)
            await page.goto(search_url, wait_until="domcontentloaded",
                            timeout=int(self.timeout * 1000))
            await asyncio.sleep(1.5)

            # Step 2: Navigate to the search form page (pre-filled from session)
            await page.goto("https://booking.flypeach.com/en/search",
                            wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(1.5)

            # Step 3: Click "Search by One-way" to submit — bypasses reCAPTCHA entirely
            try:
                one_way_link = page.get_by_role("link", name=re.compile(r"Search by One-way", re.IGNORECASE))
                await one_way_link.click(timeout=10000)
                logger.info("Peach: clicked 'Search by One-way'")
            except Exception as e:
                logger.warning("Peach: could not click one-way search (%s)", e)
                return self._empty(req)

            # Wait for flight results page
            try:
                await page.wait_for_url("**/flight_search**", timeout=20000)
                logger.info("Peach: reached flight_search page")
            except Exception:
                if "flight_search" not in page.url:
                    logger.warning("Peach: did not reach flight_search (at %s)", page.url)
                    return self._empty(req)

            await asyncio.sleep(2.0)

            flights_data = await self._extract_flights_from_dom(page)

            if not flights_data:
                logger.warning("Peach: no flights extracted from DOM")
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._build_offers(flights_data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("Peach Playwright error: %s", e)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass
            if own_context:
                await context.close()

    # ------------------------------------------------------------------
    # URL building
    # ------------------------------------------------------------------

    @staticmethod
    def _build_search_url(req: FlightSearchRequest) -> str:
        """Build direct booking search URL with JSON params."""
        params = [{
            "departure_date": req.date_from.strftime("%Y/%m/%d"),
            "departure_airport_code": req.origin,
            "arrival_airport_code": req.destination,
            "is_return": False,
        }]
        json_str = json.dumps(params, separators=(",", ":"))
        encoded = urllib.parse.quote(json_str)
        return f"https://booking.flypeach.com/en/getsearch?s={encoded}"

    # ------------------------------------------------------------------
    # DOM extraction
    # ------------------------------------------------------------------

    async def _extract_flights_from_dom(self, page) -> list[dict]:
        """Extract flight data from Peach's server-rendered results page.

        DOM structure per flight row (observed from live site):
        - paragraph with flight number (MM307) and aircraft type (A320)
        - time elements: departure HH:MM, arrow, arrival HH:MM  
        - duration text (1Hour30Min(s))
        - fare cells with prices (￥3,990) and seats info (e.g. "4 seats left at this price")
        - Three fare tiers: Minimum, Standard, Standard Plus
        """
        return await page.evaluate(r"""() => {
            const results = [];
            const body = document.body.innerText || '';

            // Find all flight number occurrences (MM + 2-4 digits)
            const flightNoRegex = /MM\d{2,4}/g;
            const allMatches = [...body.matchAll(flightNoRegex)];
            const uniqueFlights = [...new Set(allMatches.map(m => m[0]))];

            // For each unique flight, find its containing element and extract data
            for (const flightNo of uniqueFlights) {
                // Find the paragraph element containing this flight number
                const fnEls = [];
                document.querySelectorAll('p').forEach(p => {
                    if (p.textContent.trim() === flightNo) fnEls.push(p);
                });
                if (fnEls.length === 0) continue;
                const fnEl = fnEls[0];

                // Walk up to find the flight row container
                let row = fnEl;
                for (let i = 0; i < 10; i++) {
                    if (!row.parentElement) break;
                    row = row.parentElement;
                    // Flight row has multiple direct children (flight info, times, fares)
                    if (row.children.length >= 4) break;
                }

                const text = row.innerText || '';

                // Aircraft type: sibling or nearby paragraph with A3XX/B7XX pattern
                let aircraft = '';
                const nearbyPs = row.querySelectorAll('p');
                nearbyPs.forEach(p => {
                    const t = p.textContent.trim();
                    if (/^[AB]\d{3}/.test(t)) aircraft = t;
                });

                // Times (HH:MM pattern)
                const timeMatches = text.match(/(\d{2}:\d{2})/g) || [];

                // Prices: ￥ followed by digits with commas
                const priceMatches = text.match(/￥([\d,]+)/g) || [];
                const prices = priceMatches.map(
                    p => parseInt(p.replace(/[￥,]/g, ''))
                );

                // Seats remaining ("N seats left")
                const seatMatches = [...text.matchAll(/(\d+)\s*seats?\s*left/gi)];
                const seats = seatMatches.map(m => parseInt(m[1]));

                // Duration (e.g. "1Hour30Min(s)")
                let durationMins = 0;
                const durMatch = text.match(/(\d+)\s*Hour\s*(\d+)\s*Min/i);
                if (durMatch) {
                    durationMins = parseInt(durMatch[1]) * 60 + parseInt(durMatch[2]);
                }

                if (flightNo && timeMatches.length >= 2) {
                    results.push({
                        flight_no: flightNo,
                        aircraft: aircraft,
                        dep_time: timeMatches[0],
                        arr_time: timeMatches[1],
                        duration_mins: durationMins,
                        prices: prices,
                        seats: seats,
                    });
                }
            }

            return results;
        }""")

    # ------------------------------------------------------------------
    # Offer building
    # ------------------------------------------------------------------

    def _build_offers(self, flights_data: list[dict], req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        booking_url = self._build_booking_url(req)

        for flight in flights_data:
            prices = [p for p in flight.get("prices", []) if p > 0]
            if not prices:
                continue
            best_price = min(prices)

            flight_no = flight.get("flight_no", "")
            dep_time = flight.get("dep_time", "")
            arr_time = flight.get("arr_time", "")
            duration_mins = flight.get("duration_mins", 0)

            dep_dt = self._time_on_date(dep_time, req.date_from)
            arr_dt = self._time_on_date(arr_time, req.date_from)

            if arr_dt < dep_dt:
                arr_dt += timedelta(days=1)

            total_dur = (
                duration_mins * 60
                if duration_mins
                else max(int((arr_dt - dep_dt).total_seconds()), 0)
            )

            seg = FlightSegment(
                airline="MM",
                airline_name="Peach Aviation",
                flight_no=flight_no,
                origin=req.origin,
                destination=req.destination,
                departure=dep_dt,
                arrival=arr_dt,
                cabin_class="M",
            )

            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=max(total_dur, 0),
                stopovers=0,
            )

            fkey = f"{flight_no}_{dep_dt.isoformat()}"
            offers.append(FlightOffer(
                id=f"mm_{hashlib.md5(fkey.encode()).hexdigest()[:12]}",
                price=round(best_price, 2),
                currency="JPY",
                price_formatted=f"¥{best_price:,.0f}",
                outbound=route,
                inbound=None,
                airlines=["Peach Aviation"],
                owner_airline="MM",
                booking_url=booking_url,
                is_locked=False,
                source="peach_direct",
                source_tier="free",
            ))

        return offers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _time_on_date(time_str: str, date) -> datetime:
        """Combine HH:MM string with a date into a datetime."""
        if not time_str:
            return datetime(2000, 1, 1)
        try:
            h, m = time_str.split(":")
            return datetime(date.year, date.month, date.day, int(h), int(m))
        except (ValueError, IndexError):
            return datetime(2000, 1, 1)

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Peach %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"peach{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="JPY", offers=offers, total_results=len(offers),
        )

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        params = json.dumps([{
            "departure_date": req.date_from.strftime("%Y/%m/%d"),
            "departure_airport_code": req.origin,
            "arrival_airport_code": req.destination,
            "is_return": False,
        }], separators=(",", ":"))
        return f"https://booking.flypeach.com/en/getsearch?s={urllib.parse.quote(params)}"

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"peach{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="JPY", offers=[], total_results=0,
        )
