"""
Virgin Australia connector — Sabre DX GraphQL real-time search.

Virgin Australia (IATA: VA) — SYD/MEL/BNE hubs.
110+ domestic and short-haul international routes (NZ, Fiji, Bali).

Strategy (Playwright + GraphQL):
  VA's booking engine runs Sabre Digital Experience (DX) at
  book.virginaustralia.com/dx/VADX/.  The SPA calls a GraphQL endpoint
  /api/graphql that returns branded fare results with full availability.

  Headless=True gets 403 (bot detection), so we use headed Chrome
  pushed off-screen with --window-position=-2400,-2400.

  1. Launch Chrome → navigate to book.virginaustralia.com/dx/VADX/#/flight-search
  2. page.evaluate(fetch('/api/graphql')) with RT MATRIX query
     (API requires round-trip; we add a dummy return leg +5 days)
  3. Parse:
     a) brandedResults.itineraryPartBrands[0] for exact-date outbound results
     b) Fallback: bundledAlternateDateOffers matching the outbound date
  4. The response uses @ref/@id deduplication for itinerary parts.

  Session setup: ~12-15s.  Each search: ~3-5s.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)
from connectors.browser import (
    _launched_pw_instances,
    acquire_browser_slot,
    release_browser_slot,
)

logger = logging.getLogger(__name__)

_BOOKING_URL = "https://book.virginaustralia.com/dx/VADX/#/flight-search"
_SESSION_MAX_AGE = 10 * 60  # Refresh session every 10 min

_GQL_QUERY = """query bookingAirSearch($airSearchInput: CustomAirSearchInput) {
  bookingAirSearch(airSearchInput: $airSearchInput) {
    originalResponse
  }
}"""

# Shared browser state (module-level singleton)
_farm_lock: Optional[asyncio.Lock] = None
_pw_instance = None
_browser = None
_page = None
_session_ts: float = 0.0


async def _get_lock() -> asyncio.Lock:
    global _farm_lock
    if _farm_lock is None:
        _farm_lock = asyncio.Lock()
    return _farm_lock


async def _ensure_session():
    """Return a live page with VA session cookies, refreshing if needed."""
    global _page, _session_ts

    age = time.monotonic() - _session_ts
    if _page and age < _SESSION_MAX_AGE:
        try:
            await _page.evaluate("1+1")
            return _page
        except Exception:
            pass

    return await _refresh_session()


async def _refresh_session():
    """Create Playwright session on VA booking SPA."""
    global _pw_instance, _browser, _page, _session_ts

    if _page:
        try:
            await _page.close()
        except Exception:
            pass
        _page = None

    if _browser:
        try:
            await _browser.close()
        except Exception:
            pass
        _browser = None

    if _pw_instance:
        try:
            await _pw_instance.stop()
        except Exception:
            pass
        _pw_instance = None

    from playwright.async_api import async_playwright

    pw = await async_playwright().start()
    _pw_instance = pw
    _launched_pw_instances.append(pw)

    browser = await pw.chromium.launch(
        headless=False, channel="chrome",
        args=[
            "--disable-blink-features=AutomationControlled",
            "--window-position=-2400,-2400",
            "--window-size=1366,768",
        ],
    )
    _browser = browser

    ctx = await browser.new_context(
        viewport={"width": 1366, "height": 768},
        locale="en-AU",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        ),
    )

    page = await ctx.new_page()
    await page.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    )

    try:
        await page.goto(_BOOKING_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)
        _page = page
        _session_ts = time.monotonic()
        logger.info("Virgin Australia: session established")
        return _page
    except Exception as e:
        logger.error("Virgin Australia: session setup failed: %s", e)
        try:
            await page.close()
        except Exception:
            pass
        return None


class VirginAustraliaConnectorClient:
    """Virgin Australia — Sabre DX GraphQL real-time search (Playwright)."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        pass  # Browser session is module-level singleton, reused across calls

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        lock = await _get_lock()
        offers: list[FlightOffer] = []

        async with lock:
            await acquire_browser_slot()
            try:
                page = await _ensure_session()
                if page:
                    raw = await self._gql_search(page, req)
                    if raw:
                        offers = self._parse(raw, req)
            except Exception as e:
                logger.error("VirginAustralia search error: %s", e)
            finally:
                release_browser_slot()

        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))
        elapsed = time.monotonic() - t0
        logger.info("VirginAustralia %s→%s: %d offers in %.1fs",
                     req.origin, req.destination, len(offers), elapsed)

        sh = hashlib.md5(f"va{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{sh}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "AUD",
            offers=offers,
            total_results=len(offers),
        )

    async def _gql_search(self, page, req: FlightSearchRequest) -> Optional[dict]:
        """Execute GraphQL bookingAirSearch via page.evaluate(fetch)."""
        dep = req.date_from.strftime("%Y-%m-%d")
        ret = (req.date_from + timedelta(days=5)).strftime("%Y-%m-%d")
        adults = req.adults or 1

        result = await page.evaluate("""async ([origin, dest, dep, ret, adults, query]) => {
            const variables = {
                airSearchInput: {
                    searchType: "MATRIX",
                    itineraryParts: [
                        {from: {code: origin}, to: {code: dest}, when: {date: dep}},
                        {from: {code: dest}, to: {code: origin}, when: {date: ret}}
                    ],
                    passengers: {ADT: adults}
                }
            };
            try {
                const resp = await fetch('/api/graphql', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'x-sabre-storefront': 'VADX'
                    },
                    body: JSON.stringify({query: query, variables: variables})
                });
                if (!resp.ok) return {error: resp.status};
                return await resp.json();
            } catch(e) { return {error: e.message}; }
        }""", [req.origin, req.destination, dep, ret, adults, _GQL_QUERY])

        if isinstance(result, dict) and "error" in result:
            logger.warning("VA GraphQL error: %s", result["error"])
            # Force session refresh on next call
            global _session_ts
            _session_ts = 0.0
            return None

        try:
            return result["data"]["bookingAirSearch"]["originalResponse"]
        except (KeyError, TypeError):
            logger.warning("VA GraphQL: unexpected response shape")
            return None

    def _parse(self, data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse Sabre DX branded results into FlightOffer list."""
        offers: list[FlightOffer] = []
        currency = data.get("currency", "AUD")
        target_date = req.date_from.strftime("%Y-%m-%d")

        # Strategy 1: brandedResults.itineraryPartBrands[0] (exact date outbound)
        branded = data.get("brandedResults", {}).get("itineraryPartBrands", [])
        if branded and len(branded) > 0 and branded[0]:
            offers = self._parse_branded(branded[0], data, req, currency, target_date)
            if offers:
                return offers

        # Strategy 2: bundledAlternateDateOffers (alternate dates matrix)
        alt_offers = data.get("bundledAlternateDateOffers", [])
        if alt_offers:
            offers = self._parse_alternate(alt_offers, req, currency, target_date)

        return offers

    def _parse_branded(self, part_brands: list, data: dict, req: FlightSearchRequest,
                       currency: str, target_date: str) -> list[FlightOffer]:
        """Parse brandedResults.itineraryPartBrands[0] — exact-date outbound flights."""
        offers: list[FlightOffer] = []
        for option in part_brands:
            if not isinstance(option, dict):
                continue
            segments_data = option.get("segments", [])
            brand_offers = option.get("brandOffers", [])
            stops = option.get("stops", 0)
            total_dur = option.get("totalDuration", 0)

            segments = self._build_segments(segments_data)
            if not segments:
                continue

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur * 60,
                stopovers=stops,
            )

            for bo in brand_offers:
                price = bo.get("totalAmount") or 0
                if float(price) <= 0:
                    continue
                brand_id = bo.get("brandId", "")

                offers.append(self._make_offer(
                    segments[0], route, float(price), currency, brand_id, req, target_date
                ))

        return offers

    def _parse_alternate(self, alt_offers: list, req: FlightSearchRequest,
                         currency: str, target_date: str) -> list[FlightOffer]:
        """Parse bundledAlternateDateOffers — multi-date matrix results."""
        offers: list[FlightOffer] = []
        # Build @id → object map for resolving @ref pointers
        id_map: dict[str, dict] = {}
        self._index_refs(alt_offers, id_map)

        seen_keys: set[str] = set()

        for offer in alt_offers:
            if not isinstance(offer, dict):
                continue
            if offer.get("status") == "UNAVAILABLE" or offer.get("soldout"):
                continue

            dep_dates = offer.get("departureDates", [])
            if not dep_dates or dep_dates[0] != target_date:
                continue

            # Extract price from total.alternatives[0][0]
            total = offer.get("total", {})
            alts = total.get("alternatives", [])
            if not alts or not alts[0]:
                continue
            price = alts[0][0].get("amount", 0)
            curr = alts[0][0].get("currency", currency)
            if float(price) <= 0:
                continue

            # Resolve outbound itinerary part (index 0)
            parts = offer.get("itineraryPart", [])
            if not parts:
                continue

            outbound_part = self._resolve_ref(parts[0], id_map)
            if not outbound_part:
                continue

            segments_data = outbound_part.get("segments", [])
            segments = self._build_segments(segments_data)
            if not segments:
                continue

            stops = outbound_part.get("stops", 0)
            total_dur = outbound_part.get("totalDuration", 0)

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur * 60,
                stopovers=stops,
            )

            brand_id = offer.get("brandId", "")
            # Dedup: same flight + brand = same offer
            dedup_key = f"{segments[0].flight_no}_{segments[0].departure}_{brand_id}"
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)

            # Price is round-trip total; halve for one-way estimate
            ow_price = round(float(price) / 2, 2)

            offers.append(self._make_offer(
                segments[0], route, ow_price, curr, brand_id, req, target_date
            ))

        return offers

    def _index_refs(self, items: list, id_map: dict):
        """Recursively index all objects with @id for @ref resolution."""
        for item in items:
            if not isinstance(item, dict):
                continue
            if "@id" in item:
                id_map[item["@id"]] = item
            for v in item.values():
                if isinstance(v, list):
                    self._index_refs(v, id_map)
                elif isinstance(v, dict) and "@id" in v:
                    id_map[v["@id"]] = v

    def _resolve_ref(self, obj: dict, id_map: dict) -> Optional[dict]:
        """Resolve a @ref pointer to its full object, or return obj if inline."""
        if "@ref" in obj:
            return id_map.get(obj["@ref"])
        return obj

    def _build_segments(self, segments_data: list) -> list[FlightSegment]:
        """Convert API segments to FlightSegment list."""
        segments: list[FlightSegment] = []
        for seg in segments_data:
            if not isinstance(seg, dict):
                continue
            fl = seg.get("flight", {})
            dep_str = seg.get("departure", "")
            arr_str = seg.get("arrival", "")

            dep_dt = datetime(2000, 1, 1)
            arr_dt = datetime(2000, 1, 1)
            try:
                dep_dt = datetime.fromisoformat(dep_str)
            except (ValueError, TypeError):
                pass
            try:
                arr_dt = datetime.fromisoformat(arr_str)
            except (ValueError, TypeError):
                pass

            airline_code = fl.get("airlineCode", "VA")
            flight_num = fl.get("flightNumber", "")

            segments.append(FlightSegment(
                airline=airline_code,
                airline_name="Virgin Australia",
                flight_no=f"{airline_code}{flight_num}",
                origin=seg.get("origin", ""),
                destination=seg.get("destination", ""),
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=seg.get("duration", 0) * 60,
                cabin_class=seg.get("cabinClass", "Economy"),
            ))
        return segments

    def _make_offer(self, first_seg: FlightSegment, route: FlightRoute,
                    price: float, currency: str, brand_id: str,
                    req: FlightSearchRequest, target_date: str) -> FlightOffer:
        fid = hashlib.md5(
            f"va_{req.origin}{req.destination}{first_seg.departure}{brand_id}{price}".encode()
        ).hexdigest()[:12]

        return FlightOffer(
            id=f"va_{fid}",
            price=round(price, 2),
            currency=currency,
            price_formatted=f"{price:,.2f} {currency}",
            outbound=route,
            inbound=None,
            airlines=["Virgin Australia"],
            owner_airline="VA",
            conditions={"fare_brand": brand_id},
            booking_url=(
                f"https://www.virginaustralia.com/au/en/"
                f"?origin={req.origin}&destination={req.destination}"
                f"&date={target_date}"
                f"&ADT={req.adults or 1}&type=O"
            ),
            is_locked=False,
            source="virginaustralia_direct",
            source_tier="free",
        )
