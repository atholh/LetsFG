"""
Travix connector — covers BudgetAir, Vayama, Vliegwinkel, CheapTickets.nl
(CDP Chrome + __NEXT_DATA__ extraction + API interception).

Travix (Booking Holdings subsidiary) is a European OTA group with consolidator
fares. Proven 581 EUR vs 822 EUR gap on test routes (CheapFlights comparison).

Strategy (CDP Chrome + response interception + __NEXT_DATA__):
1. Launch real Chrome via --remote-debugging-port.
2. Connect via Playwright CDP.
3. Navigate to BudgetAir search results URL directly.
4. Intercept JSON responses from edgeapi.travix.com or __NEXT_DATA__.
5. Parse flight results -> FlightOffers.
"""

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
    stealth_popen_kwargs,
)

logger = logging.getLogger(__name__)

_BUDGETAIR_BASE = "https://www.budgetair.co.uk"
_CDP_PORT = 9466
_USER_DATA_DIR = os.path.join(
    os.environ.get("TEMP", os.environ.get("TMPDIR", "/tmp")), ".travix_chrome_data"
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


def _parse_dt(s: Any) -> datetime:
    if not s:
        return datetime(2000, 1, 1)
    s = str(s)
    try:
        clean = s.split("+")[0] if "+" in s and "T" in s else s
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
        logger.info("Travix: Chrome launched on CDP port %d (pid %d)", _CDP_PORT, _chrome_proc.pid)
        return _browser


async def _dismiss_cookies(page) -> None:
    for label in ["Accept", "Accept all", "Accept All", "OK", "Got it", "I agree"]:
        try:
            btn = page.get_by_role("button", name=label)
            if await btn.count() > 0:
                await btn.first.click(timeout=2000)
                await asyncio.sleep(0.5)
                break
        except Exception:
            continue


class TravixConnectorClient:
    """Travix/BudgetAir - CDP Chrome + deep link + __NEXT_DATA__ / API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        return ob_result

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        date_str = req.date_from.strftime("%Y-%m-%d")
        direct_date = req.date_from.strftime("%Y%m%d")
        results_url = (
            f"{_BUDGETAIR_BASE}/flightresults?"
            f"adt={req.adults}&chd={req.children}&cls=Y&inf={req.infants}"
            f"&out0_dep_all=false&out0_arr_all=false"
            f"&out0_dep={req.origin}&out0_arr={req.destination}&out0_date={direct_date}"
        )

        context = await _get_context()
        page = await context.new_page()
        await apply_cdp_url_blocking(page)

        captured: list[dict] = []

        async def _on_response(response):
            if response.status == 200:
                ct = response.headers.get("content-type", "")
                if "json" in ct:
                    try:
                        data = await response.json()
                        if isinstance(data, dict):
                            captured.append(data)
                    except Exception:
                        pass

        page.on("response", _on_response)

        try:
            logger.info("Travix: loading BudgetAir results URL for %s→%s on %s", req.origin, req.destination, date_str)
            await page.goto(results_url, wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            await asyncio.sleep(3.0)
            logger.info("Travix: navigated to %s", page.url[:150])

            remaining = max(self.timeout - (time.monotonic() - t0), 15)
            deadline = time.monotonic() + remaining

            offers: list[FlightOffer] = []
            while time.monotonic() < deadline and not offers:
                try:
                    page_data = await page.evaluate("""() => {
                        if (window.__NEXT_DATA__) return JSON.stringify(window.__NEXT_DATA__);
                        if (window.__APP_DATA__) return JSON.stringify(window.__APP_DATA__);
                        return null;
                    }""")
                    if page_data:
                        data = json.loads(page_data)
                        if isinstance(data, dict):
                            props = data.get("props", {}).get("pageProps", data)
                            offers = self._parse(props, req, date_str)
                            if offers:
                                break
                except Exception:
                    pass

                for data in captured:
                    offers = self._parse(data, req, date_str)
                    if offers:
                        break
                if offers:
                    break

                await asyncio.sleep(1.5)

            offers.sort(key=lambda o: o.price)
            elapsed = time.monotonic() - t0
            logger.info("Travix %s→%s: %d offers in %.1fs (CDP Chrome)", req.origin, req.destination, len(offers), elapsed)

            sh = hashlib.md5(f"travix{req.origin}{req.destination}{date_str}".encode()).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{sh}", origin=req.origin, destination=req.destination,
                currency=offers[0].currency if offers else "EUR",
                offers=offers, total_results=len(offers),
            )
        except Exception as exc:
            logger.error("Travix CDP error: %s", exc)
            return self._empty(req)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def _fill_search_form(self, page, req: FlightSearchRequest) -> bool:
        target = req.date_from

        try:
            cookie_btn = page.locator("[data-testid='cookieModal.acceptButton']")
            if await cookie_btn.count() > 0:
                await cookie_btn.first.click(timeout=3000)
                await asyncio.sleep(1.0)
                logger.info("Travix: accepted cookies")
        except Exception:
            pass

        try:
            ow = page.locator("[data-testid='searchbox.flightType.oneWay']")
            if await ow.count() > 0:
                await ow.first.click(timeout=3000)
                logger.info("Travix: clicked One way")
                await asyncio.sleep(0.5)
        except Exception:
            pass

        ok = await self._fill_airport(page, "departure", req.origin)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        ok = await self._fill_airport(page, "destination", req.destination)
        if not ok:
            return False
        await asyncio.sleep(0.5)

        ok = await self._fill_date(page, target)
        if not ok:
            return False

        return True

    async def _fill_airport(self, page, field_type: str, iata: str) -> bool:
        try:
            input_testid = f"searchbox.{field_type}.input-0"
            field = page.locator(f"[data-testid='{input_testid}']")
            if await field.count() == 0:
                if field_type == "departure":
                    field = page.locator("#DEPARTURE_AIRPORT")
                else:
                    field = page.locator("#DESTINATION_AIRPORT")
            if await field.count() == 0:
                logger.warning("Travix: could not find %s field", field_type)
                return False

            await field.first.click(timeout=3000)
            await asyncio.sleep(0.5)

            try:
                await field.first.fill("")
            except Exception:
                await page.keyboard.press("Control+a")
                await page.keyboard.press("Backspace")

            await page.keyboard.type(iata, delay=100)
            await asyncio.sleep(2.0)

            try:
                items = page.locator("[data-testid='location.airport.item']")
                count = 0
                for _ in range(8):
                    count = await items.count()
                    if count > 0:
                        break
                    await asyncio.sleep(0.25)
                if count > 0:
                    target_upper = iata.upper()
                    chosen_idx = 0
                    for idx in range(min(count, 12)):
                        txt = (await items.nth(idx).inner_text()).upper()
                        if target_upper in txt:
                            chosen_idx = idx
                            break
                    await items.nth(chosen_idx).click(timeout=3000)
                    chosen_txt = (await items.nth(chosen_idx).inner_text()).strip().replace("\n", " ")
                    logger.info("Travix: selected %s for %s via location.airport.item (%s)", iata, field_type, chosen_txt[:80])
                    return True
            except Exception:
                pass

            suggestion_selectors = [
                f"[data-testid*='{field_type}'] [role='option']",
                f"[data-testid='sb.dropdownPanel.{field_type}'] li",
                f"[data-testid='sb.dropdownPanel.{field_type}'] [role='option']",
                "[role='option']",
                "[data-testid*='suggestion']",
            ]
            for sel in suggestion_selectors:
                try:
                    items = page.locator(sel)
                    if await items.count() > 0:
                        await items.first.click(timeout=3000)
                        logger.info("Travix: selected %s for %s via %s", iata, field_type, sel)
                        return True
                except Exception:
                    continue

            await page.keyboard.press("Enter")
            await asyncio.sleep(0.5)
            logger.info("Travix: pressed Enter for %s (%s)", field_type, iata)
            return True
        except Exception as exc:
            logger.warning("Travix: %s field error: %s", field_type, exc)
            return False

    async def _fill_date(self, page, target) -> bool:
        try:
            month_num = target.month
            day_num = target.day
            month_name = target.strftime("%B")
            year = target.year
            day_testid = f"calendar.{month_num}.{day_num}"

            dialog = page.locator('[role="dialog"]')
            if await dialog.count() == 0:
                try:
                    opener = page.locator('[data-testid="searchbox.dates.openDatesModal-0"]').first
                    if await opener.count() > 0:
                        await opener.click(timeout=3000)
                        await asyncio.sleep(1.0)
                except Exception:
                    pass

            day_cell = page.locator(f'[data-testid="{day_testid}"]')
            if await day_cell.count() > 0:
                await day_cell.first.click(timeout=3000)
                logger.info("Travix: selected date %s-%02d-%02d (direct click)", year, month_num, day_num)
                await asyncio.sleep(0.5)
                return True

            scrolled = await page.evaluate(f"""() => {{
                const tables = document.querySelectorAll('table[aria-label]');
                for (const t of tables) {{
                    if (t.getAttribute('aria-label') === '{month_name} {year}') {{
                        t.scrollIntoView({{ behavior: 'instant', block: 'center' }});
                        return true;
                    }}
                }}
                const container = document.querySelector('[role="dialog"] [class*="sc-1ln6pvq"], [role="dialog"] > div:last-child');
                if (container) {{
                    container.scrollTop += 1500;
                    return true;
                }}
                return false;
            }}""")
            if scrolled:
                await asyncio.sleep(0.5)
                day_cell = page.locator(f'[data-testid="{day_testid}"]')
                if await day_cell.count() > 0:
                    await day_cell.first.click(timeout=3000)
                    logger.info("Travix: selected date %s-%02d-%02d (after scroll)", year, month_num, day_num)
                    await asyncio.sleep(0.5)
                    return True

            try:
                months_tab = page.locator('[data-gtm-id="sb-dates-btn-months"]')
                if await months_tab.count() > 0:
                    await months_tab.first.click(timeout=3000)
                    await asyncio.sleep(1.0)
                    month_btn = page.locator(f'text="{month_name}"').first
                    if await month_btn.count() == 0:
                        month_btn = page.locator(f':text("{month_name}")').first
                    if await month_btn.count() > 0:
                        await month_btn.click(timeout=3000)
                        await asyncio.sleep(1.0)
                        day_cell = page.locator(f'[data-testid="{day_testid}"]')
                        if await day_cell.count() > 0:
                            await day_cell.first.click(timeout=3000)
                            logger.info("Travix: selected date %s-%02d-%02d (via Months tab)", year, month_num, day_num)
                            await asyncio.sleep(0.5)
                            return True
            except Exception:
                pass

            for _ in range(12):
                await page.evaluate("""() => {
                    const containers = document.querySelectorAll('[role="dialog"] div');
                    for (const c of containers) {
                        if (c.scrollHeight > c.clientHeight && c.clientHeight > 200) {
                            c.scrollTop += 400;
                            break;
                        }
                    }
                }""")
                await asyncio.sleep(0.5)
                day_cell = page.locator(f'[data-testid="{day_testid}"]')
                if await day_cell.count() > 0:
                    await day_cell.first.click(timeout=3000)
                    logger.info("Travix: selected date %s-%02d-%02d (after repeated scroll)", year, month_num, day_num)
                    await asyncio.sleep(0.5)
                    return True

            logger.warning("Travix: could not select date %s-%02d-%02d", year, month_num, day_num)
            return False
        except Exception as exc:
            logger.warning("Travix: date fill error: %s", exc)
            return False

    def _parse(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        offers: list[FlightOffer] = []

        resp_obj = data.get("response")
        flights = (
            (resp_obj.get("flights") if isinstance(resp_obj, dict) else None)
            or data.get("flights")
            or []
        )
        if not isinstance(flights, list) or not flights:
            return offers

        for item in flights[:50]:
            try:
                fares = item.get("fares") or []
                if not fares:
                    continue
                fare = fares[0]
                display_fare = fare.get("displayFare") or {}
                price = float(display_fare.get("total") or 0)
                if price <= 0:
                    continue
                currency = fare.get("currencyCode") or "EUR"

                top_carrier = item.get("carrier") or {}
                top_carrier_code = top_carrier.get("code") or ""
                top_carrier_name = top_carrier.get("displayName") or top_carrier_code

                outbound_opts = item.get("outboundOptions") or []
                if not outbound_opts:
                    continue
                opt = outbound_opts[0]

                seg_options = opt.get("segmentOptions") or []
                if not seg_options:
                    continue

                opt_dep = (opt.get("departureAirport") or {}).get("code") or req.origin
                opt_arr = (opt.get("arrivalAirport") or {}).get("code") or req.destination

                segments: list[FlightSegment] = []
                for idx, seg in enumerate(seg_options):
                    seg_carrier = seg.get("carrier") or {}
                    carrier_code = seg_carrier.get("code") if isinstance(seg_carrier, dict) else str(seg_carrier)
                    carrier_name = seg_carrier.get("displayName") if isinstance(seg_carrier, dict) else carrier_code
                    if not carrier_name:
                        carrier_name = top_carrier_name

                    seg_dep = seg.get("departure")
                    seg_arr = seg.get("arrival")
                    dep_code = seg_dep.get("code") if isinstance(seg_dep, dict) else (seg_dep or (opt_dep if idx == 0 else ""))
                    arr_code = seg_arr.get("code") if isinstance(seg_arr, dict) else (seg_arr or (opt_arr if idx == len(seg_options) - 1 else ""))

                    flight_no = seg.get("flightNumber") or ""
                    dep_dt = _parse_dt(seg.get("departureDateTime"))
                    arr_dt = _parse_dt(seg.get("arrivalDateTime"))

                    dur_raw = seg.get("duration") or ""
                    dur_str = str(dur_raw).zfill(4)
                    try:
                        dur_secs = int(dur_str[:2]) * 3600 + int(dur_str[2:]) * 60
                    except (ValueError, IndexError):
                        dur_secs = 0

                    segments.append(FlightSegment(
                        airline=carrier_name or top_carrier_name,
                        flight_no=f"{carrier_code}{flight_no}",
                        origin=dep_code or req.origin,
                        destination=arr_code or req.destination,
                        departure=dep_dt,
                        arrival=arr_dt,
                        duration_seconds=dur_secs,
                    ))

                if not segments:
                    continue

                total_dur = sum(segment.duration_seconds for segment in segments)
                route = FlightRoute(segments=segments, total_duration_seconds=total_dur, stopovers=max(0, len(segments) - 1))
                oid = hashlib.md5(f"trvx_{req.origin}{req.destination}{date_str}{price}{segments[0].flight_no}".encode()).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"trvx_{oid}",
                    price=round(price, 2),
                    currency=currency,
                    price_formatted=f"{price:.2f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=list({segment.airline for segment in segments if segment.airline}),
                    owner_airline=segments[0].airline if segments else "BudgetAir",
                    booking_url=f"{_BUDGETAIR_BASE}/en-gb/flights/results/oneway/{req.origin}/{req.destination}/{date_str}/1/0/0/economy",
                    is_locked=False,
                    source="travix_ota",
                    source_tier="free",
                ))
            except Exception as exc:
                logger.debug("Travix parse error: %s", exc)

        return offers

    @staticmethod
    def _combine_rt(ob: list[FlightOffer], ib: list[FlightOffer], req) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for outbound_offer in ob[:15]:
            for inbound_offer in ib[:10]:
                price = round(outbound_offer.price + inbound_offer.price, 2)
                cid = hashlib.md5(f"{outbound_offer.id}_{inbound_offer.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_trvx_{cid}",
                    price=price,
                    currency=outbound_offer.currency,
                    outbound=outbound_offer.outbound,
                    inbound=inbound_offer.outbound,
                    airlines=list(dict.fromkeys(outbound_offer.airlines + inbound_offer.airlines)),
                    owner_airline=outbound_offer.owner_airline,
                    booking_url=outbound_offer.booking_url,
                    is_locked=False,
                    source=outbound_offer.source,
                    source_tier=outbound_offer.source_tier,
                ))
        combos.sort(key=lambda combo: combo.price)
        return combos[:20]

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"travix{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}", origin=req.origin, destination=req.destination,
            currency="EUR", offers=[], total_results=0,
        )