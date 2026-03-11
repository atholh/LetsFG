"""
Wizzair direct connector — uses Playwright to bypass Kasada bot protection.

Wizzair protects their /Api/search/search endpoint with Kasada (KPSDK),
which requires JavaScript execution to solve proof-of-work challenges.
curl_cffi / httpx get 429'd regardless of TLS fingerprint.

Strategy: Launch headless Chromium → navigate to the Wizzair search page →
let Wizzair's SPA solve Kasada and make the API call → intercept + parse
the response.  The browser instance is reused across searches.

This is the DEFINITIVE source for Wizzair pricing — no middleman markup.
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

# ── Anti-fingerprint pools (randomised per search) ─────────────────────────
_VIEWPORTS = [
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 1920, "height": 1080},
    {"width": 1280, "height": 720},
    {"width": 1600, "height": 900},
]
_LOCALES = ["en-GB", "en-US", "en-IE", "en-AU", "en-CA"]
_TIMEZONES = [
    "Europe/Warsaw", "Europe/London", "Europe/Berlin",
    "Europe/Paris", "Europe/Rome", "Europe/Madrid",
]

# ── Shared browser singleton ──────────────────────────────────────────────
_pw_instance = None
_browser = None
_browser_lock = asyncio.Lock() if hasattr(asyncio, "Lock") else None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Return a shared headless Chromium instance (launched once, reused)."""
    global _pw_instance, _browser

    lock = _get_lock()
    async with lock:
        if _browser and _browser.is_connected():
            return _browser

        from playwright.async_api import async_playwright

        _pw_instance = await async_playwright().start()
        # Use system Chrome — less detectable than Playwright's bundled Chromium.
        # Headed mode required: Kasada blocks headless browsers entirely
        # ("Human Verification" page).
        try:
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                channel="chrome",
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            # Fallback if Chrome not installed — use bundled Chromium headed
            _browser = await _pw_instance.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
        logger.info("Wizzair: Playwright browser launched (headed Chrome)")
        return _browser


class WizzairConnectorClient:
    """Wizzair connector using Playwright to bypass Kasada bot protection."""

    def __init__(self, timeout: float = 45.0):
        self.timeout = timeout

    async def close(self):
        # Browser is a shared singleton — don't close it here.
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """
        Search Wizzair flights using Playwright to bypass Kasada.

        Flow:
        1. Open homepage (lets Kasada JS initialize, sets tokens)
        2. Hash-navigate to the search route (SPA triggers API call)
        3. Intercept the search API response
        """
        t0 = time.monotonic()

        # Hash fragment for the search route
        date_out = req.date_from.isoformat()
        date_in = req.return_from.isoformat() if req.return_from else ""
        search_hash = (
            f"/booking/select-flight/"
            f"{req.origin}/{req.destination}/{date_out}/{date_in}/"
            f"{req.adults}/{req.children or 0}/{req.infants or 0}"
        )

        browser = await _get_browser()

        # Fresh context per search — randomise fingerprint so repeated
        # searches don't look like the same visitor (defeats dynamic pricing).
        context = await browser.new_context(
            viewport=random.choice(_VIEWPORTS),
            locale=random.choice(_LOCALES),
            timezone_id=random.choice(_TIMEZONES),
            service_workers="block",
            color_scheme=random.choice(["light", "dark", "no-preference"]),
        )

        try:
            # Apply stealth to avoid detection
            try:
                from playwright_stealth import stealth_async
                page = await context.new_page()
                await stealth_async(page)
            except ImportError:
                page = await context.new_page()

            # Disable browser-level HTTP cache via CDP so no cached
            # responses or etags leak between searches.
            try:
                cdp = await context.new_cdp_session(page)
                await cdp.send("Network.setCacheDisabled", {"cacheDisabled": True})
            except Exception:
                pass  # CDP not available on all setups — context isolation still helps

            # Set up response interception
            captured_data: dict = {}
            api_response_event = asyncio.Event()

            async def on_response(response):
                try:
                    if "/Api/search/search" in response.url and response.status == 200:
                        captured_data["json"] = await response.json()
                        api_response_event.set()
                except Exception:
                    pass

            page.on("response", on_response)

            # Step 1: Load homepage (Kasada JS initializes here)
            logger.info("Wizzair: loading homepage for %s→%s", req.origin, req.destination)
            await page.goto(
                "https://wizzair.com/en-gb",
                wait_until="networkidle",
                timeout=int(self.timeout * 1000),
            )

            # Nuke all client-side storage that could track repeat visits
            # (dynamic pricing relies on cookies + localStorage signals).
            # We keep only Kasada's session tokens which were just set.
            await page.evaluate("""() => {
                try { sessionStorage.clear(); } catch {}
                try {
                    const dominated = Object.keys(localStorage).filter(
                        k => !k.startsWith('kpsdk') && !k.startsWith('_kas')
                    );
                    dominated.forEach(k => localStorage.removeItem(k));
                } catch {}
            }""")

            # Step 2: Hash-navigate to trigger search (SPA route change)
            await page.evaluate(f'window.location.hash = "{search_hash}"')

            # Step 3: Wait for the search API response
            try:
                await asyncio.wait_for(
                    api_response_event.wait(),
                    timeout=max(self.timeout - (time.monotonic() - t0), 10),
                )
            except asyncio.TimeoutError:
                # Retry: dismiss cookie consent if present, then wait more
                logger.debug("Wizzair: retrying after potential overlay")
                for selector in [
                    "button[data-test='cookie-policy-button-accept']",
                    "[class*='cookie'] button",
                    "button:has-text('Accept')",
                ]:
                    try:
                        btn = page.locator(selector).first
                        if await btn.is_visible(timeout=1000):
                            await btn.click()
                            break
                    except Exception:
                        continue

                try:
                    await asyncio.wait_for(api_response_event.wait(), timeout=15)
                except asyncio.TimeoutError:
                    logger.warning("Wizzair: timed out waiting for search response")
                    return self._empty(req)

            data = captured_data.get("json", {})
            if not data:
                logger.warning("Wizzair: captured empty response")
                return self._empty(req)

            elapsed = time.monotonic() - t0

            outbound_parsed = self._parse_flights(data.get("outboundFlights", []))
            return_parsed = self._parse_flights(data.get("returnFlights", []))

            offers = self._build_offers(req, outbound_parsed, return_parsed)

            logger.info(
                "Wizzair %s→%s returned %d offers in %.1fs (Playwright)",
                req.origin, req.destination, len(offers), elapsed,
            )

            search_hash_id = hashlib.md5(
                f"wizzair{req.origin}{req.destination}{req.date_from}".encode()
            ).hexdigest()[:12]

            return FlightSearchResponse(
                search_id=f"fs_{search_hash_id}",
                origin=req.origin,
                destination=req.destination,
                currency=req.currency,
                offers=offers,
                total_results=len(offers),
            )

        except Exception as e:
            logger.error("Wizzair Playwright error: %s", e)
            return self._empty(req)
        finally:
            await context.close()

    def _build_offers(
        self,
        req: FlightSearchRequest,
        outbound_parsed: list[dict],
        return_parsed: list[dict],
    ) -> list[FlightOffer]:
        """Build FlightOffer objects from parsed flight data."""
        offers = []

        if req.return_from and return_parsed:
            outbound_parsed.sort(key=lambda x: x["price"])
            return_parsed.sort(key=lambda x: x["price"])

            for ob in outbound_parsed[:15]:
                for rt in return_parsed[:10]:
                    total = ob["price"] + rt["price"]
                    offer = FlightOffer(
                        id=f"w6_{hashlib.md5((ob['key'] + rt['key']).encode()).hexdigest()[:12]}",
                        price=round(total, 2),
                        currency=ob.get("currency", req.currency),
                        price_formatted=f"{total:.2f} {ob.get('currency', req.currency)}",
                        outbound=ob["route"],
                        inbound=rt["route"],
                        airlines=["Wizz Air"],
                        owner_airline="W6",
                        booking_url=self._build_booking_url(req),
                        is_locked=False,
                        source="wizzair_direct",
                        source_tier="free",
                    )
                    offers.append(offer)
        else:
            for ob in outbound_parsed:
                offer = FlightOffer(
                    id=f"w6_{hashlib.md5(ob['key'].encode()).hexdigest()[:12]}",
                    price=round(ob["price"], 2),
                    currency=ob.get("currency", req.currency),
                    price_formatted=f"{ob['price']:.2f} {ob.get('currency', req.currency)}",
                    outbound=ob["route"],
                    inbound=None,
                    airlines=["Wizz Air"],
                    owner_airline="W6",
                    booking_url=self._build_booking_url(req),
                    is_locked=False,
                    source="wizzair_direct",
                    source_tier="free",
                )
                offers.append(offer)

        offers.sort(key=lambda o: o.price)
        return offers

    def _parse_flights(self, flights: list[dict]) -> list[dict]:
        """Parse Wizzair flight entries into intermediate format."""
        results = []
        for flight in flights:
            fares = flight.get("fares", [])
            if not fares:
                continue

            # Get the basic fare (cheapest bundle)
            best_price = float("inf")
            best_currency = "EUR"
            for fare in fares:
                bundle = fare.get("bundle", "")
                base = fare.get("basePrice", {})
                amount = float(base.get("amount", 0))
                currency = base.get("currencyCode", "EUR")

                # Also check discounted price (WDC price)
                disc = fare.get("discountedPrice", {})
                disc_amount = float(disc.get("amount", 0)) if disc else 0

                effective = disc_amount if disc_amount > 0 else amount
                if 0 < effective < best_price:
                    best_price = effective
                    best_currency = currency

            if best_price == float("inf") or best_price <= 0:
                continue

            # Build segments
            dep_str = flight.get("departureDateTime", "")
            arr_str = flight.get("arrivalDateTime", "")
            flight_num = flight.get("flightNumber", "").replace(" ", "")

            dep_dt = self._parse_dt(dep_str)
            arr_dt = self._parse_dt(arr_str)

            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0

            route = FlightRoute(
                segments=[FlightSegment(
                    airline="W6",
                    airline_name="Wizz Air",
                    flight_no=flight_num,
                    origin=flight.get("departureStation", ""),
                    destination=flight.get("arrivalStation", ""),
                    departure=dep_dt,
                    arrival=arr_dt,
                    cabin_class="M",
                )],
                total_duration_seconds=max(dur, 0),
                stopovers=0,
            )

            key = f"{flight_num}_{dep_str}"

            results.append({
                "price": best_price,
                "currency": best_currency,
                "key": key,
                "route": route,
            })

        return results

    def _parse_dt(self, s: str) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    async def _rotate_session(self) -> None:
        pass  # Not needed — Playwright handles sessions

    def _build_booking_url(self, req: FlightSearchRequest) -> str:
        date_out = req.date_from.isoformat()
        date_in = req.return_from.isoformat() if req.return_from else ""
        return (
            f"https://wizzair.com/en-gb#/booking/select-flight/"
            f"{req.origin}/{req.destination}/{date_out}/{date_in}/"
            f"{req.adults}/{req.children}/{req.infants}"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"wizzair{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )
