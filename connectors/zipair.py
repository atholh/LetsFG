"""
ZIPAIR Playwright connector — browser automation to bypass WAF.

ZIPAIR Tokyo (IATA: ZG) is a Japanese LCC (JAL group).
Website: www.zipair.net — English version at /en.

Homepage observations (Mar 2026):
- Round Trip / One Way toggle
- Origin pre-filled with TokyoNRT (country/region selector)
- Destination field
- "Search Flight" button
- Cookie: "Agree" button

Strategy:
1. Navigate to zipair.net/en homepage
2. Dismiss cookie "Agree" banner
3. Fill search form (From/To/Departure, One-way)
4. Intercept API responses (availability / fares)
5. Parse JSON → FlightOffer objects
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
_LOCALES = ["en-US", "en-GB", "en-JP", "ja-JP"]
_TIMEZONES = [
    "Asia/Tokyo", "Asia/Seoul", "America/Los_Angeles",
    "Asia/Bangkok", "Pacific/Honolulu",
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
        logger.info("Zipair: Playwright browser launched (headed Chrome)")
        return _browser


class ZipairConnectorClient:
    """ZIPAIR Tokyo Playwright connector — homepage form search + API interception."""

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
            color_scheme=random.choice(["light", "dark", "no-preference"]),
        )
        try:
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            try:
                cdp = await context.new_cdp_session(page)
                await cdp.send("Network.setCacheDisabled", {"cacheDisabled": True})
            except Exception:
                pass

            captured_data: dict = {}
            api_event = asyncio.Event()

            async def on_response(response):
                try:
                    url = response.url.lower()
                    if response.status == 200 and (
                        "availability" in url or "/api/flights" in url
                        or "/api/search" in url or "search/flights" in url
                        or "flights/search" in url or "fares" in url
                        or "offers" in url or "low-fare" in url
                        or "flight-search" in url or "flightsearch" in url
                        or "booking" in url and "search" in url
                        or "shopping" in url
                    ):
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, (dict, list)):
                                captured_data["json"] = data
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            logger.info("Zipair: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto("https://www.zipair.net/en",
                            wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            await asyncio.sleep(3.0)

            await self._dismiss_cookies(page)
            await asyncio.sleep(0.5)
            await self._dismiss_cookies(page)

            await self._set_one_way(page)
            await asyncio.sleep(0.5)

            ok = await self._fill_airport_field(page, req.origin, 0)
            if not ok:
                logger.warning("Zipair: origin fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_airport_field(page, req.destination, 1)
            if not ok:
                logger.warning("Zipair: destination fill failed")
                return self._empty(req)
            await asyncio.sleep(0.5)

            ok = await self._fill_date(page, req)
            if not ok:
                logger.warning("Zipair: date fill failed")
                return self._empty(req)
            await asyncio.sleep(0.3)

            await self._click_search(page)

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("Zipair: timed out waiting for API response")
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
            logger.error("Zipair Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    async def _dismiss_cookies(self, page) -> None:
        for label in [
            "Agree", "Accept All", "Accept all", "Accept", "I agree",
            "Got it", "OK", "Close", "同意する",
        ]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], '
                    + '[class*="onetrust"], [id*="onetrust"], [class*="modal-overlay"], '
                    + '[class*="popup"], [id*="popup"], [class*="privacy"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    async def _set_one_way(self, page) -> None:
        for label in ["One Way", "One-way", "One way", "片道"]:
            try:
                el = page.get_by_text(label, exact=False).first
                if el and await el.count() > 0:
                    await el.click(timeout=3000)
                    return
            except Exception:
                continue
        for label in ["One Way", "One-way"]:
            try:
                radio = page.get_by_role("radio", name=re.compile(rf"{re.escape(label)}", re.IGNORECASE))
                if await radio.count() > 0:
                    await radio.first.click(timeout=2000)
                    return
            except Exception:
                continue
        try:
            toggle = page.locator("[data-testid*='one-way'], [data-testid*='oneway'], [class*='one-way']").first
            if await toggle.count() > 0:
                await toggle.click(timeout=2000)
        except Exception:
            pass

    async def _fill_airport_field(self, page, iata: str, index: int) -> bool:
        origin_labels = ["Origin", "From", "Departure", "出発地"]
        dest_labels = ["Destination", "To", "Arrival", "到着地"]
        labels = origin_labels if index == 0 else dest_labels
        try:
            for name in labels:
                for role in ["combobox", "textbox"]:
                    field = page.get_by_role(role, name=re.compile(rf"{re.escape(name)}", re.IGNORECASE))
                    if await field.count() > 0:
                        await field.first.click(timeout=3000)
                        await asyncio.sleep(0.3)
                        await field.first.fill("")
                        await asyncio.sleep(0.2)
                        await field.first.fill(iata)
                        await asyncio.sleep(2.5)
                        for role2 in ["option", "button", "listitem", "link"]:
                            try:
                                option = page.get_by_role(role2, name=re.compile(rf"{re.escape(iata)}", re.IGNORECASE)).first
                                if await option.count() > 0:
                                    await option.click(timeout=3000)
                                    return True
                            except Exception:
                                continue
                        item = page.locator(
                            "[class*='suggestion'], [class*='option'], [class*='result'], "
                            "[class*='autocomplete'] li, [class*='dropdown'] li, "
                            "[class*='airport'] li, [class*='station'] li"
                        ).filter(has_text=re.compile(rf"{re.escape(iata)}", re.IGNORECASE)).first
                        if await item.count() > 0:
                            await item.click(timeout=3000)
                            return True
                        await page.keyboard.press("Enter")
                        return True
        except Exception as e:
            logger.debug("Zipair: field %d error: %s", index, e)
        try:
            inputs = page.locator("input[type='text'], input[type='search'], input[placeholder]")
            if await inputs.count() > index:
                field = inputs.nth(index)
                await field.click(timeout=3000)
                await field.fill("")
                await asyncio.sleep(0.2)
                await field.fill(iata)
                await asyncio.sleep(2.5)
                await page.keyboard.press("Enter")
                return True
        except Exception:
            pass
        return False

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from
        try:
            for name in ["Departure", "Depart", "Date", "出発日"]:
                field = page.get_by_role("textbox", name=re.compile(rf"{re.escape(name)}", re.IGNORECASE))
                if await field.count() > 0:
                    await field.first.click(timeout=3000)
                    break
            else:
                date_el = page.locator("[class*='date'], [data-testid*='date'], [id*='date'], [class*='calendar']").first
                if await date_el.count() > 0:
                    await date_el.click(timeout=3000)
            await asyncio.sleep(0.8)
            target_my = target.strftime("%B %Y")
            for _ in range(12):
                content = await page.content()
                if target_my.lower() in content.lower():
                    break
                try:
                    fwd = page.get_by_role("button", name=re.compile(r"(next|forward|>|›)", re.IGNORECASE))
                    if await fwd.count() > 0:
                        await fwd.first.click(timeout=2000)
                        await asyncio.sleep(0.4)
                        continue
                except Exception:
                    pass
                try:
                    fwd = page.locator("[class*='next'], [aria-label*='next']").first
                    await fwd.click(timeout=2000)
                    await asyncio.sleep(0.4)
                except Exception:
                    break
            day = target.day
            for fmt in [
                f"{target.strftime('%B')} {day}, {target.year}",
                f"{day} {target.strftime('%B')} {target.year}",
                f"{target.strftime('%B')} {day}",
                target.strftime("%Y-%m-%d"),
            ]:
                try:
                    day_btn = page.locator(f"[aria-label*='{fmt}']").first
                    if await day_btn.count() > 0:
                        await day_btn.click(timeout=3000)
                        return True
                except Exception:
                    continue
            day_btn = page.locator(
                "table button, .calendar button, [class*='calendar'] button, [class*='datepicker'] button"
            ).filter(has_text=re.compile(rf"^{day}$")).first
            if await day_btn.count() > 0:
                await day_btn.click(timeout=3000)
                return True
            day_btn = page.get_by_role("button", name=re.compile(rf"^{day}$")).first
            await day_btn.click(timeout=3000)
            return True
        except Exception as e:
            logger.warning("Zipair: date error: %s", e)
            return False

    async def _click_search(self, page) -> None:
        for label in ["Search Flight", "Search Flights", "Search", "SEARCH", "フライト検索"]:
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE))
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            await page.keyboard.press("Enter")

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        try:
            await asyncio.sleep(3)
            data = await page.evaluate("""() => {
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                const scripts = document.querySelectorAll('script[type="application/json"]');
                for (const s of scripts) {
                    try { const d = JSON.parse(s.textContent);
                        if (d && (d.flights || d.journeys || d.fares || d.availability)) return d;
                    } catch {}
                }
                return null;
            }""")
            if data:
                return self._parse_response(data, req)
        except Exception:
            pass
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
            id=f"zg_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2), currency=req.currency,
            price_formatted=f"{best_price:.2f} {req.currency}",
            outbound=route, inbound=None, airlines=["ZIPAIR"], owner_airline="ZG",
            booking_url=booking_url, is_locked=False, source="zipair_direct", source_tier="free",
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
        carrier = seg.get("carrierCode") or seg.get("carrier") or seg.get("airline") or "ZG"
        return FlightSegment(
            airline=carrier, airline_name="ZIPAIR", flight_no=flight_no,
            origin=origin, destination=destination,
            departure=self._parse_dt(dep_str), arrival=self._parse_dt(arr_str), cabin_class="M",
        )

    def _build_response(self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info("Zipair %s→%s returned %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)
        h = hashlib.md5(f"zipair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
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
            f"https://www.zipair.net/en/booking?origin={req.origin}"
            f"&destination={req.destination}&departure={dep}&adults={req.adults}&tripType=OW"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"zipair{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency=req.currency, offers=[], total_results=0,
        )
