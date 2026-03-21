"""
Icelandair connector — Playwright session + instant-search API.

Icelandair (IATA: FI) is Iceland's flag carrier. Key for transatlantic routes
via KEF (Reykjavik-Keflavik) hub connecting Europe <> North America.
90+ destinations including US, Canada, and European cities.

Strategy (Cloudflare-protected — needs browser session):
  1. Launch headed Chrome → visit icelandair.com for Cloudflare cookies.
  2. Call /api/new/instant-search/streaming/ via page.evaluate(fetch).
     Params: departure=<city_code>&destinations=<city_code>
     Returns: NDJSON with cheapest round-trip offer per destination.
  3. Parse offer into FlightOffer with real flight details.

  Session setup: ~5s (cached). Each search: <1s.

API discovered Mar 2026:
  GET /api/new/instant-search/streaming/?departure=KEF&destinations=LHR
  Response (NDJSON):
    {"destination":"LHR","offer":{
       "currency":"ISK","totalFareAmount":36415,
       "outbound":{"departure":{"date":"...","location":"KEF","time":"1610"},
                    "arrival":{"date":"...","location":"LHR","time":"2020"},
                    "carriers":{"marketing":"FI","operating":"FI"},
                    "duration":"0310","flightNumber":"454"},
       "inbound":{...}
    }}
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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
from connectors.browser import _launched_pw_instances

logger = logging.getLogger(__name__)

# Map IATA airport codes → Icelandair city codes (used by instant-search API).
_IATA_TO_CITY: dict[str, str] = {
    # Multi-airport cities
    "JFK": "NYC", "EWR": "NYC", "LGA": "NYC",
    "LHR": "LON", "LGW": "LON", "STN": "LON", "LCY": "LON",
    "CDG": "PAR", "ORY": "PAR",
    "KEF": "REK", "RKV": "REK",
    "FCO": "ROM", "CIA": "ROM",
    "MXP": "MIL", "LIN": "MIL",
    "ARN": "STO", "NYO": "STO",
    "OSL": "OSL", "TRF": "OSL",
    # Single-airport → same code
}

# Shared browser session (module-level singleton)
_lock: Optional[asyncio.Lock] = None
_pw_inst = None
_browser = None
_page = None
_session_ts: float = 0.0
_SESSION_TTL = 10 * 60


def _get_lock() -> asyncio.Lock:
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


async def _ensure_session():
    """Return a page with valid Cloudflare cookies."""
    global _pw_inst, _browser, _page, _session_ts
    age = time.monotonic() - _session_ts
    if _page and age < _SESSION_TTL:
        try:
            await _page.evaluate("1+1")
            return _page
        except Exception:
            pass
    return await _refresh_session()


async def _refresh_session():
    global _pw_inst, _browser, _page, _session_ts
    for r in [_page, _browser]:
        if r:
            try:
                await r.close()
            except Exception:
                pass
    if _pw_inst:
        try:
            await _pw_inst.stop()
        except Exception:
            pass
    _page = _browser = _pw_inst = None

    from playwright.async_api import async_playwright

    pw = await async_playwright().start()
    _pw_inst = pw
    _launched_pw_instances.append(pw)

    br = await pw.chromium.launch(
        headless=False, channel="chrome",
        args=["--disable-blink-features=AutomationControlled",
              "--window-position=-2400,-2400", "--window-size=1366,768"],
    )
    _browser = br
    ctx = await br.new_context(
        viewport={"width": 1366, "height": 768}, locale="en-US",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
        ),
    )
    pg = await ctx.new_page()
    await pg.add_init_script(
        "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    )
    await pg.goto("https://www.icelandair.com/", wait_until="domcontentloaded", timeout=30000)
    await asyncio.sleep(3)
    _page = pg
    _session_ts = time.monotonic()
    logger.info("Icelandair: browser session established")
    return _page


def _city_code(iata: str) -> str:
    """Convert IATA airport code to Icelandair city code."""
    return _IATA_TO_CITY.get(iata, iata)


def _parse_duration(dur_str: str) -> int:
    """Parse 'HHMM' duration string to seconds. e.g. '0310' → 11400."""
    if not dur_str or len(dur_str) < 4:
        return 0
    try:
        h, m = int(dur_str[:2]), int(dur_str[2:4])
        return h * 3600 + m * 60
    except (ValueError, IndexError):
        return 0


def _parse_leg_dt(leg: dict) -> datetime:
    """Parse date + time from leg's departure/arrival."""
    d = leg.get("date", "")
    t = leg.get("time", "")
    if d and t and len(t) >= 4:
        try:
            return datetime.strptime(f"{d} {t[:2]}:{t[2:4]}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass
    if d:
        try:
            return datetime.strptime(d[:10], "%Y-%m-%d")
        except ValueError:
            pass
    return datetime(2000, 1, 1)


class IcelandairConnectorClient:
    """Icelandair — Playwright session + instant-search streaming API."""

    def __init__(self, timeout: float = 30.0):
        self.timeout = timeout

    async def close(self):
        pass  # Shared singleton

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        dep_city = _city_code(req.origin)
        dest_city = _city_code(req.destination)

        try:
            lock = _get_lock()
            async with lock:
                page = await _ensure_session()
            if not page:
                return self._empty(req)

            raw = await page.evaluate("""async ([dep, dest]) => {
                try {
                    const r = await fetch('/api/new/instant-search/streaming/?departure=' + dep + '&destinations=' + dest);
                    if (!r.ok) return {error: r.status};
                    return {text: await r.text()};
                } catch(e) { return {error: e.message}; }
            }""", [dep_city, dest_city])

            if "error" in raw:
                logger.warning("Icelandair API error: %s", raw["error"])
                # Session might be stale — refresh once
                async with lock:
                    page = await _refresh_session()
                if not page:
                    return self._empty(req)
                raw = await page.evaluate("""async ([dep, dest]) => {
                    try {
                        const r = await fetch('/api/new/instant-search/streaming/?departure=' + dep + '&destinations=' + dest);
                        if (!r.ok) return {error: r.status};
                        return {text: await r.text()};
                    } catch(e) { return {error: e.message}; }
                }""", [dep_city, dest_city])
                if "error" in raw:
                    return self._empty(req)

            offers = self._parse_ndjson(raw.get("text", ""), req)
            offers.sort(key=lambda o: o.price)

            elapsed = time.monotonic() - t0
            logger.info(
                "Icelandair %s→%s: %d offers in %.1fs",
                req.origin, req.destination, len(offers), elapsed,
            )
            h = hashlib.md5(
                f"icelandair{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]
            return FlightSearchResponse(
                search_id=f"fs_{h}",
                origin=req.origin,
                destination=req.destination,
                currency=offers[0].currency if offers else "USD",
                offers=offers,
                total_results=len(offers),
            )
        except Exception as e:
            logger.error("Icelandair search error: %s", e)
            return self._empty(req)

    def _parse_ndjson(self, text: str, req: FlightSearchRequest) -> list[FlightOffer]:
        """Parse NDJSON instant-search response into FlightOffers."""
        offers: list[FlightOffer] = []
        for line in text.strip().split("\n"):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue

            offer_data = obj.get("offer", {})
            price = offer_data.get("totalFareAmount")
            if not price or float(price) <= 0:
                continue

            currency = offer_data.get("currency", "USD")
            price_f = round(float(price), 2)

            outbound = offer_data.get("outbound", {})
            if not outbound:
                continue

            dep = outbound.get("departure", {})
            arr = outbound.get("arrival", {})
            carriers = outbound.get("carriers", {})
            duration_s = _parse_duration(outbound.get("duration", ""))
            fn = outbound.get("flightNumber", "")

            seg = FlightSegment(
                airline=carriers.get("marketing", "FI"),
                airline_name="Icelandair",
                flight_no=f"FI{fn}" if fn and not fn.startswith("FI") else (fn or "FI"),
                origin=dep.get("location", req.origin),
                destination=arr.get("location", req.destination),
                departure=_parse_leg_dt(dep),
                arrival=_parse_leg_dt(arr),
                duration_seconds=duration_s,
            )
            route = FlightRoute(
                segments=[seg],
                total_duration_seconds=duration_s,
                stopovers=0,
            )

            fid = hashlib.md5(
                f"fi_{fn}_{dep.get('date','')}_{price_f}".encode()
            ).hexdigest()[:12]

            booking_url = (
                f"https://www.icelandair.com/search/results"
                f"?adults={req.adults or 1}&children=0&infants=0"
                f"&isMiles=false&trips=OW"
                f"&origin={req.origin}&destination={req.destination}"
                f"&date={req.date_from.strftime('%Y-%m-%d')}"
            )

            offers.append(FlightOffer(
                id=f"fi_{fid}",
                price=price_f,
                currency=currency,
                price_formatted=f"{price_f:.0f} {currency}",
                outbound=route,
                inbound=None,
                airlines=["Icelandair"],
                owner_airline="FI",
                booking_url=booking_url,
                is_locked=False,
                source="icelandair_direct",
                source_tier="free",
            ))

        return offers

    @staticmethod
    def _empty(req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"icelandair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
