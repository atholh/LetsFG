"""
Despegar connector — Playwright + API response interception.

Despegar (NASDAQ: DESP) is Latin America's largest OTA covering all airlines.
Also operates as Decolar (Brazil), BestDay (Mexico).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from datetime import datetime, timedelta
from typing import Any, Optional

from letsfg.models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from .browser import (
    acquire_browser_slot,
    get_default_proxy,
    patchright_bandwidth_args,
    proxy_is_configured,
    release_browser_slot,
)

logger = logging.getLogger(__name__)

_AIRLINE_NAMES = {
    "AR": "Aerolineas Argentinas",
    "LA": "LATAM Airlines",
    "G3": "GOL",
    "AD": "Azul",
    "AV": "Avianca",
    "CM": "Copa Airlines",
    "JA": "JetSmart",
    "VB": "VivaAerobus",
    "Y4": "Volaris",
    "H2": "Sky Airline",
    "AA": "American Airlines",
    "UA": "United Airlines",
    "DL": "Delta Air Lines",
}

_TIME_RE = re.compile(r"^\d{2}:\d{2}$")
_PRICE_RE = re.compile(r"^(?:US\$|USD\s*)\s*([\d.,]+)$")
_AIRPORT_RE = re.compile(r"^[A-Z]{3}$")
_STEALTH_INIT_SCRIPT = """
try {
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
} catch {}
try {
    if (!window.chrome) {
        window.chrome = {runtime: {}};
    }
} catch {}
"""


async def _apply_stealth(page) -> None:
    await page.evaluate(f"() => {{{_STEALTH_INIT_SCRIPT}}}")


def _parse_duration(duration_str: str) -> int:
    if not duration_str:
        return 0
    try:
        hours, minutes = (duration_str.split(":") + ["0"])[:2]
        return int(hours) * 3600 + int(minutes) * 60
    except (ValueError, TypeError):
        return 0


def _parse_datetime(dt_str: Any) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(str(dt_str))
    except (ValueError, TypeError):
        return None


class DespegarConnectorClient:
    """Despegar OTA — Playwright browser + API interception."""

    def __init__(self, timeout: float = 55.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()

        ob_offers = await self._search_ow(req)
        if req.return_from and ob_offers:
            ib_req = req.model_copy(
                update={
                    "origin": req.destination,
                    "destination": req.origin,
                    "date_from": req.return_from,
                    "return_from": None,
                }
            )
            ib_offers = await self._search_ow(ib_req)
            if ib_offers:
                ob_offers = self._combine_rt(ob_offers, ib_offers, req)

        ob_offers.sort(key=lambda offer: offer.price if offer.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("Despegar %s→%s: %d offers in %.1fs", req.origin, req.destination, len(ob_offers), elapsed)

        search_hash = hashlib.md5(
            f"despegar{req.origin}{req.destination}{req.date_from}{req.return_from or ''}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=ob_offers[0].currency if ob_offers else "USD",
            offers=ob_offers[:30],
            total_results=len(ob_offers),
        )

    async def _search_ow(self, req: FlightSearchRequest) -> list[FlightOffer]:
        from playwright.async_api import async_playwright

        offers: list[FlightOffer] = []
        search_data: dict = {}
        search_statuses: list[int] = []
        page_title = ""
        page_url = ""
        page_blocked = False
        page_html_diag: dict[str, Any] = {}
        date_str = req.date_from.strftime("%Y-%m-%d")

        adults = req.adults or 1
        children = req.children or 0
        infants = req.infants or 0
        search_url = (
            f"https://www.despegar.com.ar/shop/flights/results/oneway/"
            f"{req.origin}/{req.destination}/{date_str}/{adults}/{children}/{infants}"
        )

        await acquire_browser_slot()
        try:
            async with async_playwright() as playwright:
                launch_kwargs: dict[str, Any] = {
                    "user_data_dir": "",
                    "headless": False,
                    "viewport": {"width": 1920, "height": 1080},
                    "locale": "es-AR",
                    "timezone_id": "America/Buenos_Aires",
                    "args": [
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        *patchright_bandwidth_args(),
                    ],
                }
                if proxy_is_configured():
                    launch_kwargs["proxy"] = get_default_proxy()

                context = await playwright.chromium.launch_persistent_context(**launch_kwargs)
                await context.add_init_script(_STEALTH_INIT_SCRIPT)
                logger.info(
                    "Despegar launch mode proxy=%s url=%s",
                    bool(launch_kwargs.get("proxy")),
                    search_url,
                )

                page = context.pages[0] if context.pages else await context.new_page()

                async def capture_response(response):
                    nonlocal search_data
                    if "flights-busquets/api/v1/web/search" not in response.url:
                        return
                    try:
                        search_statuses.append(response.status)
                        if response.status == 200:
                            data = await response.json()
                            if "items" in data and len(data.get("items", [])) > 0:
                                search_data = data
                                logger.info(
                                    "Despegar captured search payload %s->%s items=%d",
                                    req.origin,
                                    req.destination,
                                    len(data.get("items", [])),
                                )
                    except Exception as exc:
                        logger.debug("Despegar capture error: %s", exc)

                page.on("response", capture_response)

                await page.goto("https://www.despegar.com.ar/", wait_until="domcontentloaded", timeout=30000)
                await _apply_stealth(page)
                await asyncio.sleep(3)
                await page.mouse.move(240, 180)
                await asyncio.sleep(1)
                try:
                    cookie_btn = await page.query_selector(
                        'button:has-text("Aceptar"), button:has-text("Accept"), '
                        '[class*="accept" i], [id*="accept" i]'
                    )
                    if cookie_btn:
                        await cookie_btn.click(timeout=2000)
                        await asyncio.sleep(1)
                except Exception:
                    pass

                await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                await _apply_stealth(page)
                for _ in range(25):
                    await asyncio.sleep(1)
                    if search_data and len(search_data.get("items", [])) > 0:
                        break

                page_url = page.url
                page_title = await page.title()
                page_text = await page.evaluate("() => document.body?.innerText?.slice(0, 1500) || ''")
                page_blocked = any(token in page_text.lower() for token in ("datadome", "captcha", "blocked", "verify"))
                if not search_data:
                    page_html_diag = await page.evaluate(
                        """() => {
                            const html = document.documentElement?.outerHTML || '';
                            return {
                                readyState: document.readyState,
                                htmlLength: html.length,
                                hasNextData: Boolean(document.querySelector('#__NEXT_DATA__')),
                                hasScriptTags: document.querySelectorAll('script').length,
                                hasNoScript: document.querySelectorAll('noscript').length,
                                hasIframe: document.querySelectorAll('iframe').length,
                                hasDataDomeText: /datadome/i.test(html),
                                hasCaptchaText: /captcha|cf-chl|challenge-platform|verify you are human/i.test(html),
                                htmlHead: html.slice(0, 500),
                            };
                        }"""
                    )

                if not search_data:
                    offers = await self._extract_offers_from_dom(page, req, date_str)

                await context.close()

            if search_data:
                offers = self._parse_search_response(search_data, req, date_str)
                if not offers:
                    items = search_data.get("items", [])
                    first = items[0] if items else {}
                    inner = first.get("item", {}) if isinstance(first, dict) else {}
                    logger.warning(
                        "Despegar parse-empty %s->%s item_keys=%s inner_item_keys=%s",
                        req.origin,
                        req.destination,
                        sorted(first.keys())[:20] if isinstance(first, dict) else [],
                        sorted(inner.keys())[:20] if isinstance(inner, dict) else [],
                    )
            else:
                logger.warning(
                    "Despegar empty search %s->%s title=%r url=%r blocked=%s search_statuses=%s html_diag=%s",
                    req.origin,
                    req.destination,
                    page_title,
                    page_url,
                    page_blocked,
                    search_statuses[-5:],
                    page_html_diag,
                )
        except Exception as exc:
            logger.warning("Despegar browser error: %s", exc)
        finally:
            release_browser_slot()

        return offers

    async def _extract_offers_from_dom(self, page, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        texts = await page.evaluate(
            """() => Array.from(document.querySelectorAll('.cluster-container.COMMON'))
            .slice(0, 30)
            .map(el => (el.innerText || '').trim())"""
        )
        if not texts:
            diagnostics = await page.evaluate(
                """() => ({
                    clusterCommon: document.querySelectorAll('.cluster-container.COMMON').length,
                    clusterAny: document.querySelectorAll('.cluster-container').length,
                    results: document.querySelectorAll('[class*="results-cluster"], [class*="result-card"], [class*="cluster"]')
                        .length,
                    bodySample: (document.body?.innerText || '').slice(0, 600)
                })"""
            )
            logger.warning(
                "Despegar DOM fallback empty %s->%s diag=%s",
                req.origin,
                req.destination,
                diagnostics,
            )
        offers: list[FlightOffer] = []
        for index, text in enumerate(texts):
            offer = self._parse_dom_cluster(str(text), req, date_str, index)
            if offer is not None:
                offers.append(offer)
        if offers:
            logger.info("Despegar DOM fallback %s→%s: %d offers", req.origin, req.destination, len(offers))
        return offers

    def _parse_dom_cluster(self, cluster_text: str, req: FlightSearchRequest, date_str: str, index: int) -> FlightOffer | None:
        lines = [line.strip() for line in cluster_text.splitlines() if line.strip()]
        airports = [
            line for line in lines
            if _AIRPORT_RE.fullmatch(line) and line not in {"IDA", "VUE", "RT"}
        ]

        price = None
        for line in reversed(lines):
            match = _PRICE_RE.match(line.replace("\xa0", " "))
            if match:
                price = float(match.group(1).replace(".", "").replace(",", "."))
                break
        if len(airports) < 2 or price is None:
            return None

        if "Equipaje" not in lines:
            return None
        chunk = lines[lines.index("Equipaje") + 1:]
        first_time_idx = next((i for i, line in enumerate(chunk) if _TIME_RE.fullmatch(line)), None)
        if first_time_idx is None or first_time_idx == 0 or first_time_idx + 3 >= len(chunk):
            return None

        airline_name = chunk[first_time_idx - 1]
        dep_time = chunk[first_time_idx]
        stop_text = chunk[first_time_idx + 1]
        arr_time = chunk[first_time_idx + 2]
        duration_text = chunk[first_time_idx + 3]

        dep_dt = datetime.combine(req.date_from, datetime.strptime(dep_time, "%H:%M").time())
        arr_dt = datetime.combine(req.date_from, datetime.strptime(arr_time, "%H:%M").time())
        if arr_dt < dep_dt:
            arr_dt += timedelta(days=1)

        duration_match = re.search(r"(\d+)\s*h\s*(\d+)\s*m", duration_text)
        duration_seconds = 0
        if duration_match:
            duration_seconds = int(duration_match.group(1)) * 3600 + int(duration_match.group(2)) * 60

        stop_match = re.search(r"(\d+)", stop_text)
        stopovers = 0 if "Directo" in stop_text else int(stop_match.group(1)) if stop_match else 1

        segment = FlightSegment(
            airline=airline_name,
            flight_no=airline_name,
            origin=airports[0],
            destination=airports[1],
            departure=dep_dt,
            arrival=arr_dt,
            duration_seconds=duration_seconds,
        )
        route = FlightRoute(
            segments=[segment],
            total_duration_seconds=duration_seconds,
            stopovers=stopovers,
        )

        offer_id = hashlib.md5(f"dom_{req.origin}{req.destination}{date_str}{price}{index}".encode()).hexdigest()[:12]
        return FlightOffer(
            id=f"desp_dom_{offer_id}",
            price=round(price, 2),
            currency=req.currency or "USD",
            price_formatted=f"{req.currency or 'USD'} {price:.2f}",
            outbound=route,
            inbound=None,
            airlines=[airline_name],
            owner_airline=airline_name,
            booking_url=(
                f"https://www.despegar.com.ar/shop/flights/results/oneway/"
                f"{req.origin}/{req.destination}/{date_str}/{req.adults or 1}/0/0"
            ),
            is_locked=False,
            source="despegar_ota",
            source_tier="free",
        )

    def _parse_search_response(self, data: dict, req: FlightSearchRequest, date_str: str) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        currency_code = data.get("initialCurrency", "ARS")

        for wrapper in data.get("items", []):
            try:
                item = wrapper.get("item", {}) if isinstance(wrapper, dict) else {}
                if not item:
                    continue

                price_detail = item.get("priceDetail", {})
                main_fare = price_detail.get("mainFare", {})
                price = main_fare.get("amount", 0)
                currency = price_detail.get("currencyCode", currency_code)
                if price <= 0:
                    continue

                route_choices = item.get("routeChoices", [])
                if not route_choices:
                    continue
                outbound_choice = route_choices[0]
                routes = outbound_choice.get("routes", [])
                if not routes:
                    continue

                route = routes[0]
                segments_data = route.get("segments", [])
                airline_codes = item.get("airlines", [])
                validating = item.get("validatingCarrier", airline_codes[0] if airline_codes else "")

                segments: list[FlightSegment] = []
                for segment_data in segments_data:
                    dep_info = segment_data.get("departure", {})
                    arr_info = segment_data.get("arrival", {})
                    airline_code = segment_data.get("airlineCode", validating)
                    flight_id = segment_data.get("flightId", "")
                    duration_secs = _parse_duration(segment_data.get("duration", ""))

                    dep_dt = _parse_datetime(dep_info.get("date"))
                    arr_dt = _parse_datetime(arr_info.get("date"))
                    if not dep_dt:
                        dep_dt = datetime.combine(req.date_from, datetime.min.time().replace(hour=8))
                    if not arr_dt:
                        arr_dt = dep_dt

                    airline_name = _AIRLINE_NAMES.get(airline_code, airline_code)
                    segments.append(
                        FlightSegment(
                            airline=airline_name,
                            flight_no=flight_id,
                            origin=dep_info.get("airportCode", req.origin),
                            destination=arr_info.get("airportCode", req.destination),
                            departure=dep_dt,
                            arrival=arr_dt,
                            duration_seconds=duration_secs,
                        )
                    )

                if not segments:
                    continue

                total_duration = _parse_duration(route.get("totalDuration", ""))
                if not total_duration:
                    total_duration = sum(segment.duration_seconds for segment in segments)

                flight_route = FlightRoute(
                    segments=segments,
                    total_duration_seconds=total_duration,
                    stopovers=route.get("stopsCount", len(segments) - 1),
                )

                airline_names = [_AIRLINE_NAMES.get(code, code) for code in airline_codes] if airline_codes else [segments[0].airline]
                offer_id = hashlib.md5(
                    f"desp_{req.origin}{req.destination}{date_str}{price}{segments[0].flight_no}".encode()
                ).hexdigest()[:12]

                offers.append(
                    FlightOffer(
                        id=f"desp_{offer_id}",
                        price=round(float(price), 2),
                        currency=currency,
                        price_formatted=f"{float(price):,.0f} {currency}",
                        outbound=flight_route,
                        inbound=None,
                        airlines=airline_names,
                        owner_airline=validating,
                        booking_url=(
                            f"https://www.despegar.com.ar/shop/flights/results/oneway/"
                            f"{req.origin}/{req.destination}/{date_str}/{req.adults or 1}/0/0"
                        ),
                        is_locked=False,
                        source="despegar_ota",
                        source_tier="free",
                    )
                )
            except Exception as exc:
                logger.debug("Error parsing Despegar item: %s", exc)

        return offers

    @staticmethod
    def _combine_rt(ob: list[FlightOffer], ib: list[FlightOffer], req: FlightSearchRequest) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for outbound_offer in ob[:15]:
            for inbound_offer in ib[:10]:
                price = round(outbound_offer.price + inbound_offer.price, 2)
                combo_id = hashlib.md5(f"{outbound_offer.id}_{inbound_offer.id}".encode()).hexdigest()[:12]
                combos.append(
                    FlightOffer(
                        id=f"rt_desp_{combo_id}",
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
                    )
                )
        combos.sort(key=lambda offer: offer.price)
        return combos[:20]