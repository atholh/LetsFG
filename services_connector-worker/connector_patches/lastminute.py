from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Any, Optional

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import (
    _launched_procs,
    apply_cdp_url_blocking,
    bandwidth_saving_args,
    disable_background_networking_args,
    find_chrome,
    proxy_chrome_args,
    stealth_popen_kwargs,
)

logger = logging.getLogger(__name__)

_BASE = "https://www.lastminute.com"
_CDP_PORT = 9464
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".lastminute_chrome_data"
)

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None
_context = None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


def _parse_dt(value: Any) -> datetime:
    if not value:
        return datetime(2000, 1, 1)
    text = str(value)
    try:
        clean = text.split("+")[0] if "+" in text and "T" in text else text
        clean = clean.split(".")[0] if "." in clean else clean
        return datetime.fromisoformat(clean)
    except (ValueError, AttributeError):
        return datetime(2000, 1, 1)


async def _get_context():
    global _context
    browser = await _get_browser()
    if _context:
        try:
            if _context.pages:
                return _context
        except Exception:
            pass
    contexts = browser.contexts
    if contexts:
        _context = contexts[0]
    else:
        _context = await browser.new_context(viewport={"width": 1366, "height": 768})
    return _context


async def _get_browser():
    global _pw_instance, _browser, _chrome_proc
    lock = _get_lock()
    async with lock:
        if _browser:
            try:
                if _browser.is_connected():
                    return _browser
            except Exception:
                pass

        from playwright.async_api import async_playwright

        pw = None
        try:
            pw = await async_playwright().start()
            _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
            _pw_instance = pw
            logger.info("Lastminute: connected to existing Chrome on port %d", _CDP_PORT)
            return _browser
        except Exception:
            if pw:
                try:
                    await pw.stop()
                except Exception:
                    pass

        chrome = find_chrome()
        os.makedirs(_USER_DATA_DIR, exist_ok=True)
        args = [
            chrome,
            f"--remote-debugging-port={_CDP_PORT}",
            f"--user-data-dir={_USER_DATA_DIR}",
            "--no-first-run",
            *proxy_chrome_args(),
            "--no-default-browser-check",
            "--disable-blink-features=AutomationControlled",
            "--disable-http2",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
            *bandwidth_saving_args(),
            *disable_background_networking_args(),
            "about:blank",
        ]
        _chrome_proc = subprocess.Popen(args, **stealth_popen_kwargs())
        _launched_procs.append(_chrome_proc)
        await asyncio.sleep(2.0)

        pw = await async_playwright().start()
        _pw_instance = pw
        _browser = await pw.chromium.connect_over_cdp(f"http://127.0.0.1:{_CDP_PORT}")
        logger.info("Lastminute: Chrome launched on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


async def _dismiss_cookies(page) -> None:
    try:
        dismissed = await page.evaluate(
            """() => {
                const banner = document.getElementById('iubenda-cs-banner') || document.querySelector('[role="alertdialog"]');
                if (!banner) return false;
                const btn = banner.querySelector('button.iubenda-cs-accept-btn') || banner.querySelector('.iubenda-cs-accept-btn');
                if (btn) { btn.click(); return true; }
                const buttons = banner.querySelectorAll('button');
                for (const b of buttons) {
                    const text = (b.textContent || '').toLowerCase();
                    if (text.includes('accept') || text.includes('continue') || text.includes('agree') || text.includes('ok')) {
                        b.click();
                        return true;
                    }
                }
                return false;
            }"""
        )
        if dismissed:
            await asyncio.sleep(1.0)
            logger.debug("Lastminute: dismissed iubenda cookie banner via JS")
            return
    except Exception:
        pass
    for label in ["Accept & continue", "Accept", "Accept all", "Accept All", "I agree", "Got it", "OK"]:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await asyncio.sleep(0.5)
                break
        except Exception:
            continue


class LastminuteConnectorClient:
    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        outbound_result = await self._search_ow(req)
        if req.return_from and outbound_result.total_results > 0:
            inbound_req = req.model_copy(update={
                "origin": req.destination,
                "destination": req.origin,
                "date_from": req.return_from,
                "return_from": None,
            })
            inbound_result = await self._search_ow(inbound_req)
            if inbound_result.total_results > 0:
                outbound_result.offers = self._combine_rt(outbound_result.offers, inbound_result.offers, req)
                outbound_result.total_results = len(outbound_result.offers)
        return outbound_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")

        context = await _get_context()
        page = await context.new_page()
        await apply_cdp_url_blocking(page)

        search_data: dict = {}

        async def _on_response(response):
            url = response.url
            if "/api/v3/availability" in url and response.status == 200:
                try:
                    data = await response.json()
                    if isinstance(data, dict) and ("flights" in data or "numberOfResults" in data):
                        search_data.update(data)
                        logger.info("Lastminute: captured availability API (%d bytes)", len(json.dumps(data)))
                except Exception as exc:
                    logger.debug("Lastminute: failed to parse availability response: %s", exc)

        page.on("response", _on_response)

        try:
            logger.info("Lastminute: loading homepage for %s→%s on %s", req.origin, req.destination, date_str)
            await page.goto(f"{_BASE}/flights", wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            try:
                await page.wait_for_selector('input[role="combobox"][aria-label="Departure"]', timeout=10000)
            except Exception:
                logger.debug("Lastminute: departure field did not appear within initial wait")
            await asyncio.sleep(2.0)

            await _dismiss_cookies(page)
            await asyncio.sleep(0.5)

            ok = await self._fill_search_form(page, req)
            if not ok:
                logger.warning("Lastminute: form fill failed")
                return self._empty(req)

            clicked = False
            for sel in (
                'button[aria-label="Find"]',
                'button:has-text("Find")',
                'button:has-text("Search")',
                'button[type="submit"]',
            ):
                try:
                    btn = page.locator(sel).first
                    if await btn.count() > 0:
                        await btn.click(timeout=5000)
                        clicked = True
                        logger.info("Lastminute: clicked search button via %s", sel)
                        break
                except Exception:
                    continue
            if not clicked:
                logger.warning("Lastminute: could not click search button")
                return self._empty(req)

            try:
                await page.wait_for_url("**/flights/search/**", timeout=15000)
                logger.info("Lastminute: navigated to %s", page.url)
            except Exception:
                logger.debug("Lastminute: didn't navigate to search results, URL: %s", page.url)

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining
            while not search_data and time.monotonic() < deadline:
                await asyncio.sleep(0.5)

            if not search_data:
                logger.warning("Lastminute: no availability data captured")
                return self._empty(req)

            offers = self._parse(search_data, req, date_str)
            offers.sort(key=lambda offer: offer.price)
            elapsed = time.monotonic() - t0
            logger.info("Lastminute %s→%s: %d offers in %.1fs (CDP Chrome)", req.origin, req.destination, len(offers), elapsed)

            search_hash = hashlib.md5(f"lastminute{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{search_hash}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else "EUR",
                offers=offers,
                total_results=len(offers),
            )
        except Exception as exc:
            logger.error("Lastminute CDP error: %s", exc)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        if not await self._fill_airport(page, "Departure", req.origin):
            return False
        await asyncio.sleep(0.5)

        if not await self._fill_airport(page, "Destination", req.destination):
            return False
        await asyncio.sleep(0.5)

        return await self._fill_date(page, req.date_from)

    async def _fill_airport(self, page, label: str, iata: str) -> bool:
        try:
            field = page.get_by_role("combobox", name=label).first
            if await field.count() == 0:
                field = page.locator(f'input[role="combobox"][aria-label="{label}"]').first
            if await field.count() == 0:
                field = page.locator(f'[aria-label="{label}"]').first
            if await field.count() == 0:
                field = page.get_by_placeholder(label).first
            if await field.count() == 0:
                logger.warning("Lastminute: could not find %s field", label)
                return False

            try:
                await field.click(force=True, timeout=3000)
            except Exception:
                await field.evaluate("(el) => el.focus()")
            await asyncio.sleep(0.3)

            clear_btn = page.get_by_role("button", name=f"Clear {label} field").first
            if await clear_btn.count() > 0:
                try:
                    await clear_btn.click(force=True, timeout=2000)
                    await asyncio.sleep(0.2)
                except Exception:
                    pass

            try:
                await field.evaluate(
                    """(el) => {
                        el.scrollIntoView({ block: 'center' });
                        el.focus();
                        el.value = '';
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    }"""
                )
            except Exception:
                try:
                    await field.click(force=True, timeout=2000)
                except Exception:
                    pass
                await page.keyboard.press("Control+a")
                await page.keyboard.press("Backspace")

            await page.keyboard.type(iata, delay=80)
            await asyncio.sleep(1.2)

            for sel in (
                '[role="option"]',
                '[role="listbox"] li',
                'ul[role="listbox"] > *',
                '[class*="suggestion"] li',
                '[class*="autocomplete"] li',
                '[class*="dropdown"] li',
            ):
                try:
                    item = page.locator(sel).first
                    if await item.count() > 0:
                        await item.click(timeout=3000)
                        logger.info("Lastminute: selected %s for %s", iata, label)
                        return True
                except Exception:
                    continue

            await page.keyboard.press("Enter")
            await asyncio.sleep(0.3)
            logger.info("Lastminute: pressed Enter for %s (%s)", label, iata)
            return True
        except Exception as exc:
            logger.warning("Lastminute: %s field error: %s", label, exc)
            return False

    async def _fill_date(self, page, target) -> bool:
        try:
            date_field = page.locator('[aria-label="Dates"]')
            if await date_field.count() > 0:
                await date_field.first.click(timeout=3000)
                await asyncio.sleep(1.0)
            else:
                for sel in ('[aria-label*="date" i]', '[class*="DateInput"]', 'button:has-text("When?")'):
                    try:
                        elem = page.locator(sel).first
                        if await elem.count() > 0:
                            await elem.click(timeout=3000)
                            await asyncio.sleep(1.0)
                            break
                    except Exception:
                        continue

            day_no_pad = str(target.day)
            month_name = target.strftime("%B")
            year = target.year
            weekday = target.strftime("%A")
            target_label = f"{weekday}, {day_no_pad} {month_name} {year}"

            for _ in range(12):
                try:
                    day_btn = page.locator(f'button[aria-label="{target_label}"]')
                    if await day_btn.count() > 0:
                        await day_btn.first.click(timeout=2000)
                        logger.info("Lastminute: selected date %s", target_label)
                        await asyncio.sleep(0.5)
                        return True
                except Exception:
                    pass

                try:
                    partial = page.locator(f'button[aria-label*="{day_no_pad} {month_name} {year}"]')
                    if await partial.count() > 0:
                        await partial.first.click(timeout=2000)
                        logger.info("Lastminute: selected date (partial) %s %s %s", day_no_pad, month_name, year)
                        await asyncio.sleep(0.5)
                        return True
                except Exception:
                    pass

                try:
                    next_btn = page.locator('button[aria-label="Next month"]')
                    if await next_btn.count() > 0:
                        await next_btn.click(timeout=2000)
                        await asyncio.sleep(0.5)
                    else:
                        break
                except Exception:
                    break

            logger.warning("Lastminute: could not select date %s", target_label)
            return False
        except Exception as exc:
            logger.warning("Lastminute: date fill error: %s", exc)
            return False

    def _parse(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        offers: list[FlightOffer] = []

        registry = data.get("dataRegistry") or {}
        airlines_list = registry.get("airlines") or []
        airlines_map = {}
        for airline in airlines_list:
            if isinstance(airline, dict):
                code = airline.get("code") or airline.get("iata") or ""
                name = airline.get("name") or code
                if code:
                    airlines_map[code] = name

        currency = "EUR"
        search_options = data.get("searchOptions") or {}
        if isinstance(search_options, dict):
            currency = search_options.get("currency") or "EUR"

        flights = data.get("flights") or []
        if not isinstance(flights, list):
            return offers

        for item in flights[:30]:
            try:
                itinerary = item.get("itinerary") or []
                if not isinstance(itinerary, list) or not itinerary:
                    continue

                first_itinerary = itinerary[0]
                leg_options = first_itinerary.get("legOptions") or []
                if not isinstance(leg_options, list) or not leg_options:
                    continue

                first_leg = leg_options[0]
                price = 0.0
                price_currency = currency

                prices = item.get("prices") or {}
                selected = prices.get("selected") or {}
                if isinstance(selected, dict):
                    selected_price = selected.get("price") or {}
                    selected_amount = selected_price.get("amount") or {}
                    per_passenger = selected_amount.get("perPassenger") or {}
                    if per_passenger.get("amountAsNumber"):
                        decimals = per_passenger.get("decimalDigits", 2)
                        price = per_passenger["amountAsNumber"] / (10 ** decimals)
                        price_currency = per_passenger.get("currencyCode") or currency

                if price <= 0:
                    price_obj = first_leg.get("price") or {}
                    leg_amount = price_obj.get("amount") or {}
                    per_passenger = leg_amount.get("perPassenger") or {}
                    if per_passenger.get("amountAsNumber"):
                        decimals = per_passenger.get("decimalDigits", 2)
                        price = per_passenger["amountAsNumber"] / (10 ** decimals)
                        price_currency = per_passenger.get("currencyCode") or currency

                if price <= 0:
                    continue

                segment_data = first_leg.get("segments") or first_leg.get("flightSegments") or []
                if not isinstance(segment_data, list) or not segment_data:
                    continue

                segments: list[FlightSegment] = []
                for segment in segment_data:
                    carrier_code = segment.get("displayAirlineCode") or segment.get("operatingAirlineCode") or segment.get("marketingCarrier") or ""
                    carrier_name = segment.get("displayAirlineName") or airlines_map.get(carrier_code, carrier_code)
                    flight_no = segment.get("flightNumber") or segment.get("number") or ""
                    dep_airport = segment.get("departureAirportCode") or segment.get("departureAirport") or req.origin
                    arr_airport = segment.get("arrivalAirportCode") or segment.get("arrivalAirport") or req.destination
                    dep_dt = _parse_dt(segment.get("departureTime") or segment.get("departureDateTime"))
                    arr_dt = _parse_dt(segment.get("arrivalTime") or segment.get("arrivalDateTime"))
                    duration_minutes = segment.get("duration") or 0

                    segments.append(FlightSegment(
                        airline=carrier_name or carrier_code,
                        flight_no=f"{carrier_code}{flight_no}",
                        origin=dep_airport,
                        destination=arr_airport,
                        departure=dep_dt,
                        arrival=arr_dt,
                        duration_seconds=int(duration_minutes) * 60 if duration_minutes else 0,
                    ))

                if not segments:
                    continue

                total_duration = first_leg.get("duration") or sum(segment.duration_seconds // 60 for segment in segments)
                total_duration_seconds = int(total_duration) * 60 if isinstance(total_duration, (int, float)) else 0
                route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=total_duration_seconds,
                    stopovers=max(0, len(segments) - 1),
                )
                offer_id = hashlib.md5(f"lm_{req.origin}{req.destination}{date_str}{price}{segments[0].flight_no}".encode()).hexdigest()[:12]

                cabin = {"M": "economy", "W": "premium", "C": "business", "F": "first"}.get(req.cabin_class or "M", "economy")
                offers.append(FlightOffer(
                    id=f"lm_{offer_id}",
                    price=round(price, 2),
                    currency=price_currency,
                    price_formatted=f"{price:.2f} {price_currency}",
                    outbound=route,
                    inbound=None,
                    airlines=list({segment.airline for segment in segments if segment.airline}),
                    owner_airline=segments[0].airline if segments else "Lastminute",
                    booking_url=f"{_BASE}/flights/{req.origin}-{req.destination}/{date_str}/1adults/{cabin}/oneway",
                    is_locked=False,
                    source="lastminute_ota",
                    source_tier="free",
                ))
            except Exception as exc:
                logger.debug("Lastminute parse error: %s", exc)

        return offers

    @staticmethod
    def _combine_rt(outbound_offers: list[FlightOffer], inbound_offers: list[FlightOffer], req) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for outbound in outbound_offers[:15]:
            for inbound in inbound_offers[:10]:
                price = round(outbound.price + inbound.price, 2)
                combo_id = hashlib.md5(f"{outbound.id}_{inbound.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_lm_{combo_id}",
                    price=price,
                    currency=outbound.currency,
                    outbound=outbound.outbound,
                    inbound=inbound.outbound,
                    airlines=list(dict.fromkeys(outbound.airlines + inbound.airlines)),
                    owner_airline=outbound.owner_airline,
                    booking_url=outbound.booking_url,
                    is_locked=False,
                    source=outbound.source,
                    source_tier=outbound.source_tier,
                ))
        combos.sort(key=lambda combo: combo.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(f"lastminute{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency="EUR",
            offers=[],
            total_results=0,
        )