"""
GOL Linhas Aéreas Playwright connector — sessionStorage injection + API interception.

GOL (IATA: G3) is Brazil's largest low-cost carrier.
Website: b2c.voegol.com.br — Angular SPA booking flow.

Strategy (token/auth interception approach):
1. Navigate to b2c.voegol.com.br/compra — Angular boots, creates auth token
2. Extract sessionStorage UUID (format: {uuid}_@SiteGolB2C:*)
3. Inject search params into sessionStorage (Angular's expected format)
4. Navigate to /compra/selecao-de-voo2/ida — Angular resolver fires BFF search
5. Intercept POST bff-flight.voegol.com.br/flights/search response (200 JSON)
6. Parse offers → FlightOffer objects

Angular's interceptors handle auth automatically:
- AuthInterceptor adds Bearer token from create-token API
- SabreCookieInterceptor adds x-sabre-cookie-encoded header
- withCredentials: true for cookie-based session
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
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
]
_TIMEZONES = [
    "America/Sao_Paulo", "America/Bahia",
    "America/Fortaleza", "America/Recife",
]

_GOL_BASE = "https://b2c.voegol.com.br"
_GOL_BFF = "bff-flight.voegol.com.br/flights/search"

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
        logger.info("GOL: Playwright browser launched (headed Chrome)")
        return _browser


class GolConnectorClient:
    """GOL connector — sessionStorage injection triggers Angular's BFF search."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale="pt-BR",
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
                    if _GOL_BFF in response.url and response.status == 200:
                        ct = response.headers.get("content-type", "")
                        if "json" in ct:
                            data = await response.json()
                            if data and isinstance(data, dict) and "offers" in data:
                                captured_data["json"] = data
                                api_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Load Angular app to get auth token + session UUID
            logger.info("GOL: loading Angular app for %s→%s", req.origin, req.destination)
            await page.goto(f"{_GOL_BASE}/compra",
                            wait_until="domcontentloaded", timeout=int(self.timeout * 1000))
            await asyncio.sleep(5)

            # Dismiss cookie/LGPD overlays
            await self._dismiss_cookies(page)

            # Step 2: Extract session UUID from sessionStorage
            uuid = await page.evaluate("""() => {
                for (let i = 0; i < sessionStorage.length; i++) {
                    const key = sessionStorage.key(i);
                    const m = key.match(/^([0-9a-f-]+)_@SiteGolB2C/);
                    if (m) return m[1];
                }
                return null;
            }""")
            if not uuid:
                logger.warning("GOL: failed to extract session UUID")
                return self._empty(req)
            logger.info("GOL: session UUID=%s", uuid[:8])

            # Step 3: Inject search params into sessionStorage
            dep_date = req.date_from.isoformat()
            is_roundtrip = req.return_from is not None

            itinerary_parts = [{
                "from": {"code": req.origin, "useNearbyLocations": False},
                "to": {"code": req.destination, "useNearbyLocations": False},
                "when": {"date": f"{dep_date}T00:00:00"},
            }]
            if is_roundtrip and req.return_from:
                ret_date = req.return_from.isoformat()
                itinerary_parts.append({
                    "from": {"code": req.destination, "useNearbyLocations": False},
                    "to": {"code": req.origin, "useNearbyLocations": False},
                    "when": {"date": f"{ret_date}T00:00:00"},
                })

            search_payload = {
                "promocodebanner": False,
                "destinationCountryToUSA": False,
                "lastSearchCourtesyTicket": False,
                "passengerCourtesyType": None,
                "airSearch": {
                    "cabinClass": None,
                    "currency": None,
                    "pointOfSale": "BR",
                    "awardBooking": False,
                    "searchType": "BRANDED",
                    "promoCodes": [""],
                    "originalItineraryParts": itinerary_parts,
                    "itineraryParts": itinerary_parts,
                    "passengers": {
                        "ADT": req.adults,
                        "TEEN": 0,
                        "CHD": req.children,
                        "INF": req.infants,
                        "UNN": 0,
                    },
                },
            }
            journey_type = "round-trip" if is_roundtrip else "one-way"
            passengers = {
                "ADT": req.adults, "TEEN": 0,
                "CHD": req.children, "INF": req.infants, "UNN": 0,
            }

            await page.evaluate("""({uuid, search, journey, passengers}) => {
                sessionStorage.setItem(uuid + '_@SiteGolB2C:search', JSON.stringify(search));
                sessionStorage.setItem(uuid + '_@SiteGolB2C:search-properties', JSON.stringify({journey: journey}));
                sessionStorage.setItem(uuid + '_@SiteGolB2C:passengers', JSON.stringify(passengers));
                sessionStorage.setItem('flightSelectionScreen', JSON.stringify('v2'));
            }""", {
                "uuid": uuid,
                "search": search_payload,
                "journey": journey_type,
                "passengers": passengers,
            })
            logger.info("GOL: sessionStorage injected")

            # Step 4: Navigate to results page — Angular resolver fires BFF search
            await page.goto(f"{_GOL_BASE}/compra/selecao-de-voo2/ida",
                            wait_until="domcontentloaded", timeout=int(self.timeout * 1000))

            remaining = max(self.timeout - (time.monotonic() - t0), 10)
            try:
                await asyncio.wait_for(api_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning("GOL: timed out waiting for BFF search response")
                return self._empty(req)

            data = captured_data.get("json", {})
            if not data:
                return self._empty(req)

            elapsed = time.monotonic() - t0
            offers = self._parse_response(data, req)
            return self._build_response(offers, req, elapsed)

        except Exception as e:
            logger.error("GOL Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    # ── Cookie / LGPD overlay dismissal ─────────────────────────────────────

    async def _dismiss_cookies(self, page) -> None:
        # GOL uses LGPD consent banner and OneTrust
        for sel in [
            "button:has-text('Continuar e fechar')",
            "button:has-text('Continue and close')",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0 and await el.is_visible():
                    await el.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    return
            except Exception:
                continue
        # Fallback: nuke overlays via DOM
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], '
                    + '[class*="onetrust"], [id*="onetrust"], [class*="lgpd"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── Response parsing (GOL BFF format) ───────────────────────────────────
    #
    # GOL BFF response structure:
    # {
    #   "execution": "uuid",
    #   "offers": [
    #     {
    #       "itinerary": {
    #         "origin": "GRU", "destination": "GIG",
    #         "departure": "2026-03-20T06:00:00",
    #         "arrival": "2026-03-20T07:05:00",
    #         "stops": 0, "totalDuration": 65,
    #         "operatingAirlineCode": ["G3"], "airlineCode": ["G3"]
    #       },
    #       "segments": [
    #         {
    #           "origin": "GRU", "destination": "GIG",
    #           "departure": "...", "arrival": "...",
    #           "fareBasis": "PNFAAG2J", "duration": 65,
    #           "flightNumber": 2044,
    #           "airlineCode": "G3", "operatingAirlineCode": "G3"
    #         }
    #       ],
    #       "fareFamily": [
    #         {
    #           "shoppingBasketHashCode": 1433343908,
    #           "brandId": "LI",  # LI=Light, CL=Classic, FL=Full
    #           "price": {
    #             "currency": "BRL", "total": 2197.54,
    #             "fare": 2163.9, "taxes": 33.64
    #           }
    #         }
    #       ]
    #     }
    #   ],
    #   "alternateDateOffers": [...],
    #   "error": null
    # }

    def _parse_response(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        raw_offers = data.get("offers", [])
        if not raw_offers:
            return []

        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        for offer_data in raw_offers:
            parsed = self._parse_offer(offer_data, req, booking_url)
            if parsed:
                offers.append(parsed)

        return offers

    def _parse_offer(
        self, offer_data: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        itinerary = offer_data.get("itinerary", {})
        fare_family = offer_data.get("fareFamily", [])
        segments_raw = offer_data.get("segments", [])

        # Find cheapest fare (LI = Light is typically cheapest)
        best_price = float("inf")
        best_currency = "BRL"
        for fare in fare_family:
            price_info = fare.get("price", {})
            total = price_info.get("total")
            if total is not None and 0 < total < best_price:
                best_price = total
                best_currency = price_info.get("currency", "BRL")

        if best_price == float("inf") or best_price <= 0:
            return None

        # Parse segments
        segments: list[FlightSegment] = []
        for seg in segments_raw:
            segments.append(FlightSegment(
                airline=seg.get("operatingAirlineCode", "G3"),
                airline_name="GOL",
                flight_no=f"G3{seg.get('flightNumber', '')}",
                origin=seg.get("origin", req.origin),
                destination=seg.get("destination", req.destination),
                departure=self._parse_dt(seg.get("departure", "")),
                arrival=self._parse_dt(seg.get("arrival", "")),
                duration_seconds=seg.get("duration", 0) * 60,
                cabin_class="M",
            ))

        if not segments:
            return None

        total_dur = itinerary.get("totalDuration", 0) * 60  # minutes → seconds
        stops = itinerary.get("stops", max(len(segments) - 1, 0))

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=total_dur,
            stopovers=stops,
        )

        # Build unique ID from flight numbers + departure
        dep = itinerary.get("departure", "")
        flight_nums = "-".join(str(s.get("flightNumber", "")) for s in segments_raw)
        offer_key = f"{dep}_{flight_nums}"

        return FlightOffer(
            id=f"g3_{hashlib.md5(offer_key.encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency=best_currency,
            price_formatted=f"{best_price:.2f} {best_currency}",
            outbound=route,
            inbound=None,
            airlines=list(set(s.airline for s in segments)),
            owner_airline="G3",
            booking_url=booking_url,
            is_locked=False,
            source="gol_direct",
            source_tier="protocol",
        )

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "GOL %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        h = hashlib.md5(
            f"gol{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else req.currency,
            offers=offers,
            total_results=len(offers),
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
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[:len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"{_GOL_BASE}/compra/selecao-de-voo2/ida"
            f"?origin={req.origin}&destination={req.destination}"
            f"&departure={dep}&adults={req.adults}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"gol{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
