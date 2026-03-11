"""
Allegiant Air Playwright connector — CDP real Chrome for Cloudflare bypass.

Allegiant (IATA: G4) is a US ultra-low-cost carrier operating leisure routes
from smaller US cities to vacation destinations (Las Vegas, Florida, etc.).

Website: www.allegiantair.com — behind aggressive Cloudflare (blocks
automation flags AND non-US IPs). Requires real Chrome via CDP subprocess
with persistent profile to preserve Cloudflare clearance cookies.

Allegiant's SPA is React-based. The booking search form uses:
- Trip type radio buttons (Round Trip / One Way)
- City-pair airport selectors (autocomplete dropdowns with city names)
- Calendar date picker (custom React component)
- Search button → triggers internal API call

Strategy:
1. Launch real Chrome subprocess + connect via CDP (bypasses Cloudflare)
2. Navigate to allegiantair.com with persistent profile (keeps CF cookies)
3. Wait for Cloudflare challenge to resolve (up to 30s)
4. Set up API response interception for flight/availability/fare endpoints
5. Fill search form: one-way → origin → destination → date → search
6. Parse intercepted API JSON → FlightOffer objects
7. Fallback: DOM extraction from page JS globals or result cards
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import re
import subprocess
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
]
_LOCALES = ["en-US"]
_TIMEZONES = [
    "America/New_York", "America/Chicago", "America/Denver",
    "America/Los_Angeles", "America/Phoenix",
]

_pw_instance = None
_browser = None
_chrome_proc = None
_browser_lock: Optional[asyncio.Lock] = None

_USER_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".allegiant_chrome_data"
)
_DEBUG_PORT = 9337

# Allegiant uses city names, not IATA codes, in their search form
_IATA_TO_CITY: dict[str, str] = {
    "LAS": "Las Vegas",
    "LAX": "Los Angeles",
    "SFB": "Orlando Sanford",
    "PIE": "St. Pete-Clearwater",
    "PGD": "Punta Gorda",
    "IWA": "Phoenix Mesa",
    "AZA": "Phoenix Mesa",
    "BLI": "Bellingham",
    "OGD": "Ogden",
    "SFO": "San Francisco",
    "OAK": "Oakland",
    "CVG": "Cincinnati",
    "PIT": "Pittsburgh",
    "IND": "Indianapolis",
    "DSM": "Des Moines",
    "ABE": "Allentown",
    "BNA": "Nashville",
    "ATL": "Atlanta",
    "DFW": "Dallas",
    "IAH": "Houston",
    "DEN": "Denver",
    "MSP": "Minneapolis",
    "DTW": "Detroit",
    "STL": "St. Louis",
    "MCI": "Kansas City",
    "CLE": "Cleveland",
    "CMH": "Columbus",
    "RDU": "Raleigh",
    "RIC": "Richmond",
    "JAX": "Jacksonville",
    "RSW": "Fort Myers",
    "TPA": "Tampa",
    "FLL": "Fort Lauderdale",
    "MIA": "Miami",
    "MCO": "Orlando",
    "SRQ": "Sarasota",
    "AUS": "Austin",
    "SAT": "San Antonio",
    "BOS": "Boston",
    "ORD": "Chicago",
    "MDW": "Chicago",
    "MSY": "New Orleans",
    "MYR": "Myrtle Beach",
    "SAV": "Savannah",
    "CHS": "Charleston",
    "SAN": "San Diego",
    "SEA": "Seattle",
    "PDX": "Portland",
    "PHX": "Phoenix",
    "TUS": "Tucson",
    "ABQ": "Albuquerque",
    "ELP": "El Paso",
    "OMA": "Omaha",
    "MEM": "Memphis",
    "BHM": "Birmingham",
    "GSP": "Greenville",
    "LEX": "Lexington",
    "SDF": "Louisville",
    "GRR": "Grand Rapids",
    "FNT": "Flint",
    "LAN": "Lansing",
    "TOL": "Toledo",
    "DAY": "Dayton",
    "FWA": "Fort Wayne",
    "SBN": "South Bend",
    "PIA": "Peoria",
    "MLI": "Moline",
    "CID": "Cedar Rapids",
    "FAR": "Fargo",
    "BIS": "Bismarck",
    "RAP": "Rapid City",
    "BIL": "Billings",
    "GFK": "Grand Forks",
    "FSD": "Sioux Falls",
    "RST": "Rochester",
    "HNL": "Honolulu",
    "OGG": "Maui",
    "LIH": "Kauai",
    "KOA": "Kona",
    "SJU": "San Juan",
    "CUN": "Cancun",
    "PVR": "Puerto Vallarta",
    "MBJ": "Montego Bay",
}

_MAX_ATTEMPTS = 2


def _find_chrome() -> Optional[str]:
    """Find Chrome executable on the system."""
    candidates = [
        os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
        os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium-browser",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    return None


def _get_lock() -> asyncio.Lock:
    global _browser_lock
    if _browser_lock is None:
        _browser_lock = asyncio.Lock()
    return _browser_lock


async def _get_browser():
    """Launch real Chrome via subprocess + connect via CDP.

    This avoids Playwright's automation flags that trigger Cloudflare.
    A persistent user-data-dir keeps Cloudflare clearance cookies.
    Falls back to regular Playwright launch if Chrome is not found.
    """
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

        if _pw_instance:
            try:
                await _pw_instance.stop()
            except Exception:
                pass
        _pw_instance = await async_playwright().start()

        chrome_path = _find_chrome()
        if chrome_path:
            os.makedirs(_USER_DATA_DIR, exist_ok=True)
            # Try connecting to an already-running Chrome debug port
            try:
                _browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://localhost:{_DEBUG_PORT}"
                )
                logger.info("Allegiant: connected to existing Chrome via CDP")
                return _browser
            except Exception:
                pass

            vp = random.choice(_VIEWPORTS)
            _chrome_proc = subprocess.Popen(
                [
                    chrome_path,
                    f"--remote-debugging-port={_DEBUG_PORT}",
                    f"--user-data-dir={_USER_DATA_DIR}",
                    f"--window-size={vp['width']},{vp['height']}",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-background-networking",
                    "about:blank",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            await asyncio.sleep(2.5)
            try:
                _browser = await _pw_instance.chromium.connect_over_cdp(
                    f"http://localhost:{_DEBUG_PORT}"
                )
                logger.info("Allegiant: connected to real Chrome via CDP (no automation flags)")
                return _browser
            except Exception as e:
                logger.warning("Allegiant: CDP connect failed: %s, falling back", e)
                if _chrome_proc:
                    _chrome_proc.terminate()
                    _chrome_proc = None

        # Fallback: regular Playwright (may get blocked by Cloudflare)
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
        logger.info("Allegiant: Playwright browser launched (headed Chrome, fallback)")
        return _browser


class AllegiantConnectorClient:
    """Allegiant Air Playwright connector — CDP Chrome + Cloudflare bypass."""

    def __init__(self, timeout: float = 60.0):
        self.timeout = timeout

    async def close(self):
        pass

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        browser = await _get_browser()

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            # Reuse existing context from CDP Chrome (keeps CF cookies)
            context = (
                browser.contexts[0]
                if browser.contexts
                else await browser.new_context(
                    viewport=random.choice(_VIEWPORTS),
                    locale=random.choice(_LOCALES),
                    timezone_id=random.choice(_TIMEZONES),
                )
            )
            page = await context.new_page()
            try:
                from playwright_stealth import stealth_async
                await stealth_async(page)
            except ImportError:
                pass

            try:
                result = await self._attempt_search(page, req, t0)
                if result and result.total_results > 0:
                    return result
                logger.warning(
                    "Allegiant: attempt %d/%d returned no results",
                    attempt, _MAX_ATTEMPTS,
                )
            except Exception as e:
                logger.warning("Allegiant: attempt %d/%d error: %s", attempt, _MAX_ATTEMPTS, e)
            finally:
                await page.close()

        return self._empty(req)

    async def _attempt_search(
        self, page, req: FlightSearchRequest, t0: float
    ) -> FlightSearchResponse:
        """Single search attempt on the Allegiant website."""

        captured_data: dict = {}
        api_event = asyncio.Event()

        async def on_response(response):
            try:
                url = response.url.lower()
                if response.status != 200:
                    return
                # Allegiant's SPA calls internal APIs for search results
                hit = (
                    "availability" in url or "/api/flights" in url
                    or "/api/search" in url or "search/flights" in url
                    or "flights/search" in url or "/api/fares" in url
                    or "/wapi/" in url or "low-fare" in url
                    or "flight-search" in url or "flightsearch" in url
                    or "booking/flights" in url or "/schedule" in url
                    or "/graphql" in url or "/api/offer" in url
                )
                if not hit:
                    return
                ct = response.headers.get("content-type", "")
                if "json" not in ct:
                    return
                data = await response.json()
                if data and isinstance(data, (dict, list)):
                    captured_data["json"] = data
                    api_event.set()
                    logger.info("Allegiant: captured API response from %s", url[:120])
            except Exception:
                pass

        page.on("response", on_response)

        # Navigate to homepage
        logger.info("Allegiant: loading homepage for %s→%s", req.origin, req.destination)
        await page.goto(
            "https://www.allegiantair.com/",
            wait_until="domcontentloaded",
            timeout=int(self.timeout * 1000),
        )

        # Wait for Cloudflare challenge to resolve
        cf_ok = await self._wait_cloudflare(page, timeout=30)
        if not cf_ok:
            logger.warning("Allegiant: Cloudflare blocked — cannot proceed")
            return self._empty(req)

        await asyncio.sleep(2.0)
        await self._dismiss_overlays(page)
        await asyncio.sleep(0.5)

        # Set one-way trip type
        await self._set_one_way(page)
        await asyncio.sleep(0.5)

        # Fill origin
        ok = await self._fill_airport(page, req.origin, is_origin=True)
        if not ok:
            logger.warning("Allegiant: origin fill failed")
            return self._empty(req)
        await asyncio.sleep(0.8)

        # Fill destination
        ok = await self._fill_airport(page, req.destination, is_origin=False)
        if not ok:
            logger.warning("Allegiant: destination fill failed")
            return self._empty(req)
        await asyncio.sleep(0.8)

        # Fill date
        ok = await self._fill_date(page, req)
        if not ok:
            logger.warning("Allegiant: date fill failed")
            return self._empty(req)
        await asyncio.sleep(0.5)

        # Click search
        await self._click_search(page)

        # Wait for API response
        remaining = max(self.timeout - (time.monotonic() - t0), 10)
        try:
            await asyncio.wait_for(api_event.wait(), timeout=min(remaining, 25))
        except asyncio.TimeoutError:
            logger.warning("Allegiant: API intercept timed out, trying DOM extraction")
            offers = await self._extract_from_dom(page, req)
            if offers:
                elapsed = time.monotonic() - t0
                return self._build_response(offers, req, elapsed)
            return self._empty(req)

        data = captured_data.get("json", {})
        if not data:
            return self._empty(req)

        elapsed = time.monotonic() - t0
        offers = self._parse_response(data, req)
        return self._build_response(offers, req, elapsed)

    # ── Cloudflare handling ──────────────────────────────────────────────

    async def _wait_cloudflare(self, page, timeout: int = 30) -> bool:
        """Wait for Cloudflare challenge to resolve. Returns True if passed."""
        for i in range(timeout // 2):
            title = (await page.title()).lower()
            if "cloudflare" not in title and "attention" not in title and "blocked" not in title:
                logger.info("Allegiant: Cloudflare passed after %ds", i * 2)
                return True
            await asyncio.sleep(2)
        return False

    # ── Overlay/cookie dismissal ─────────────────────────────────────────

    async def _dismiss_overlays(self, page) -> None:
        """Dismiss cookie banners, CCPA notices, and modal overlays."""
        for label in [
            "Accept All", "Accept all", "Accept", "I agree",
            "Got it", "OK", "Close", "Dismiss", "Accept Cookies",
            "I Understand", "Continue",
        ]:
            try:
                btn = page.get_by_role(
                    "button",
                    name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE),
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=2000)
                    await asyncio.sleep(0.5)
                    break
            except Exception:
                continue
        # Force-remove overlay elements
        try:
            await page.evaluate("""() => {
                document.querySelectorAll(
                    '[class*="cookie"], [id*="cookie"], [class*="consent"], [id*="consent"], '
                    + '[class*="onetrust"], [id*="onetrust"], [class*="modal-overlay"], '
                    + '[class*="popup"], [id*="popup"], [class*="privacy"], [class*="ccpa"], '
                    + '[class*="overlay"][style*="z-index"]'
                ).forEach(el => { if (el.offsetHeight > 0) el.remove(); });
                document.body.style.overflow = 'auto';
            }""")
        except Exception:
            pass

    # ── Trip type ────────────────────────────────────────────────────────

    async def _set_one_way(self, page) -> None:
        """Select One Way trip type."""
        # Try radio button / tab / label
        for text in ["One-way", "One Way", "One way", "ONE WAY"]:
            try:
                radio = page.get_by_role("radio", name=re.compile(rf"{re.escape(text)}", re.IGNORECASE))
                if await radio.count() > 0:
                    await radio.first.click(timeout=3000)
                    logger.info("Allegiant: selected one-way (radio)")
                    return
            except Exception:
                continue
        # Clickable label/button/tab
        for text in ["One-way", "One Way", "One way"]:
            try:
                el = page.get_by_role("tab", name=re.compile(rf"{re.escape(text)}", re.IGNORECASE))
                if await el.count() > 0:
                    await el.first.click(timeout=3000)
                    logger.info("Allegiant: selected one-way (tab)")
                    return
            except Exception:
                continue
        # Generic text click
        for text in ["One-way", "One Way", "One way"]:
            try:
                el = page.locator(
                    "label, button, div[role='button'], span"
                ).filter(has_text=re.compile(rf"^{re.escape(text)}$", re.IGNORECASE)).first
                if await el.count() > 0:
                    await el.click(timeout=3000)
                    logger.info("Allegiant: selected one-way (label)")
                    return
            except Exception:
                continue
        # data-testid fallback
        try:
            toggle = page.locator(
                "[data-testid*='one-way'], [data-testid*='oneway'], "
                "[class*='one-way'], [class*='oneway']"
            ).first
            if await toggle.count() > 0:
                await toggle.click(timeout=2000)
        except Exception:
            pass

    # ── Airport field fill ───────────────────────────────────────────────

    async def _fill_airport(self, page, iata: str, is_origin: bool) -> bool:
        """Fill origin or destination airport using city name + IATA matching."""
        city = _IATA_TO_CITY.get(iata.upper(), "")
        search_terms = [city, iata] if city else [iata]
        label_text = "from" if is_origin else "to"

        # Strategy 1: Find by role (combobox/textbox) with airport labels
        origin_names = ["From", "Departing", "Origin", "Departure", "Where from"]
        dest_names = ["To", "Arriving", "Destination", "Arrival", "Where to"]
        names = origin_names if is_origin else dest_names

        for name in names:
            for role in ["combobox", "textbox", "searchbox"]:
                try:
                    field = page.get_by_role(
                        role, name=re.compile(rf"{re.escape(name)}", re.IGNORECASE)
                    )
                    if await field.count() > 0:
                        result = await self._type_and_select(page, field.first, search_terms, iata)
                        if result:
                            return True
                except Exception:
                    continue

        # Strategy 2: Find by placeholder text
        for name in names:
            try:
                field = page.locator(
                    f"input[placeholder*='{name}' i]"
                )
                if await field.count() > 0:
                    result = await self._type_and_select(page, field.first, search_terms, iata)
                    if result:
                        return True
            except Exception:
                continue

        # Strategy 3: Find by data-testid containing origin/destination
        test_ids = (
            ["origin", "departure", "from"] if is_origin
            else ["destination", "arrival", "to"]
        )
        for tid in test_ids:
            try:
                field = page.locator(f"input[data-testid*='{tid}' i]")
                if await field.count() > 0:
                    result = await self._type_and_select(page, field.first, search_terms, iata)
                    if result:
                        return True
            except Exception:
                continue

        # Strategy 4: Find by aria-label
        for name in names:
            try:
                field = page.locator(f"input[aria-label*='{name}' i]")
                if await field.count() > 0:
                    result = await self._type_and_select(page, field.first, search_terms, iata)
                    if result:
                        return True
            except Exception:
                continue

        # Strategy 5: Positional — click on the Nth text input
        try:
            idx = 0 if is_origin else 1
            inputs = page.locator(
                "input[type='text']:visible, input[type='search']:visible, "
                "input:not([type]):visible"
            )
            count = await inputs.count()
            if count > idx:
                result = await self._type_and_select(page, inputs.nth(idx), search_terms, iata)
                if result:
                    return True
        except Exception:
            pass

        # Strategy 6: Click on a div/button that opens the airport picker
        selectors = (
            ["[data-testid*='origin']", "[class*='origin']", "[class*='departure']",
             "[class*='from-city']", "[class*='fromCity']"]
            if is_origin
            else ["[data-testid*='destination']", "[class*='destination']", "[class*='arrival']",
                  "[class*='to-city']", "[class*='toCity']"]
        )
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.click(timeout=3000)
                    await asyncio.sleep(1.0)
                    # Now look for the now-visible input
                    active_input = page.locator(
                        "input:focus, input[type='text']:visible, input[type='search']:visible"
                    ).first
                    if await active_input.count() > 0:
                        result = await self._type_and_select(
                            page, active_input, search_terms, iata
                        )
                        if result:
                            return True
            except Exception:
                continue

        logger.warning("Allegiant: could not find %s airport field", label_text)
        return False

    async def _type_and_select(
        self, page, field, search_terms: list[str], iata: str
    ) -> bool:
        """Type into a field and select from autocomplete suggestions."""
        await self._dismiss_overlays(page)
        try:
            await field.click(timeout=3000)
        except Exception:
            try:
                await field.click(force=True, timeout=3000)
            except Exception:
                return False
        await asyncio.sleep(0.5)

        for term in search_terms:
            if not term:
                continue
            try:
                # Clear + type
                await field.fill("")
                await asyncio.sleep(0.2)
                await field.press_sequentially(term, delay=60)
                await asyncio.sleep(2.5)

                # Try to find and click suggestion containing the IATA code
                if await self._click_suggestion(page, iata):
                    logger.info("Allegiant: selected airport %s (searched '%s')", iata, term)
                    return True
            except Exception:
                continue

        # Last resort: press Enter after typing
        try:
            await page.keyboard.press("Enter")
            await asyncio.sleep(0.5)
            return True
        except Exception:
            pass
        return False

    async def _click_suggestion(self, page, iata: str) -> bool:
        """Click an autocomplete suggestion matching the IATA code."""
        # Try role-based selectors
        for role in ["option", "listitem", "button", "link", "menuitem"]:
            try:
                opt = page.get_by_role(
                    role, name=re.compile(rf"\b{re.escape(iata)}\b", re.IGNORECASE)
                ).first
                if await opt.count() > 0:
                    await opt.click(timeout=3000)
                    return True
            except Exception:
                continue

        # Try CSS selectors for common dropdown/suggestion patterns
        suggestion_sel = (
            "[class*='suggestion'], [class*='option'], [class*='result'], "
            "[class*='autocomplete'] li, [class*='dropdown'] li, "
            "[class*='airport'] li, [class*='station'] li, "
            "[class*='listbox'] li, [role='listbox'] [role='option'], "
            "[class*='search-result'], [class*='city-pair'], "
            "ul li, ol li, [role='listbox'] div"
        )
        try:
            item = page.locator(suggestion_sel).filter(
                has_text=re.compile(rf"\b{re.escape(iata)}\b", re.IGNORECASE)
            ).first
            if await item.count() > 0:
                await item.click(timeout=3000)
                return True
        except Exception:
            pass

        # Try any visible element containing the IATA code in suggestion area
        try:
            item = page.locator(
                "[class*='suggest'], [class*='dropdown'], [class*='popup'], "
                "[class*='picker'], [role='listbox'], [role='menu']"
            ).locator(f"text=/{iata}/i").first
            if await item.count() > 0:
                await item.click(timeout=3000)
                return True
        except Exception:
            pass

        return False

    # ── Date picker ──────────────────────────────────────────────────────

    async def _fill_date(self, page, req: FlightSearchRequest) -> bool:
        """Fill the departure date in the calendar picker."""
        target = req.date_from
        try:
            # Open the date picker
            opened = False
            for name in ["Departure", "Depart", "Depart Date", "Date", "Travel Date",
                          "Departing", "Travel date"]:
                for role in ["textbox", "button", "combobox"]:
                    try:
                        field = page.get_by_role(
                            role, name=re.compile(rf"{re.escape(name)}", re.IGNORECASE)
                        )
                        if await field.count() > 0:
                            await field.first.click(timeout=3000)
                            opened = True
                            break
                    except Exception:
                        continue
                if opened:
                    break

            if not opened:
                # Try generic date-related selectors
                for sel in [
                    "[data-testid*='date']", "[class*='date-pick']", "[class*='datepicker']",
                    "[class*='calendar-trigger']", "[id*='date']", "[class*='depart-date']",
                    "input[type='date']",
                ]:
                    try:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            await el.click(timeout=3000)
                            opened = True
                            break
                    except Exception:
                        continue

            if not opened:
                logger.warning("Allegiant: could not open date picker")
                return False

            await asyncio.sleep(1.0)

            # Navigate to the correct month
            target_month = target.strftime("%B")
            target_year = str(target.year)
            target_my = f"{target_month} {target_year}"

            for _ in range(12):
                content = await page.content()
                if target_my.lower() in content.lower():
                    break
                # Click next month button
                fwd_clicked = False
                for sel in [
                    "button[aria-label*='next' i]",
                    "button[aria-label*='Next' i]",
                    "[class*='next-month']",
                    "[class*='nextMonth']",
                    "[class*='nav-next']",
                    "[class*='right-arrow']",
                    "[data-testid*='next']",
                ]:
                    try:
                        btn = page.locator(sel).first
                        if await btn.count() > 0:
                            await btn.click(timeout=2000)
                            fwd_clicked = True
                            break
                    except Exception:
                        continue
                if not fwd_clicked:
                    try:
                        fwd = page.get_by_role(
                            "button", name=re.compile(r"(next|forward|›|>|→)", re.IGNORECASE)
                        )
                        if await fwd.count() > 0:
                            await fwd.first.click(timeout=2000)
                    except Exception:
                        break
                await asyncio.sleep(0.5)

            # Click the target day
            day = target.day

            # Try aria-label patterns first (most reliable)
            aria_formats = [
                f"{target_month} {day}, {target_year}",
                f"{target.strftime('%A')}, {target_month} {day}, {target_year}",
                f"{day} {target_month} {target_year}",
                f"{target_month} {day}",
                target.strftime("%Y-%m-%d"),
                target.strftime("%m/%d/%Y"),
            ]
            for fmt in aria_formats:
                try:
                    btn = page.locator(f"[aria-label*='{fmt}']").first
                    if await btn.count() > 0:
                        await btn.click(timeout=3000)
                        logger.info("Allegiant: selected date via aria-label '%s'", fmt)
                        return True
                except Exception:
                    continue

            # Try data-date attribute
            try:
                date_str = target.strftime("%Y-%m-%d")
                btn = page.locator(f"[data-date='{date_str}']").first
                if await btn.count() > 0:
                    await btn.click(timeout=3000)
                    logger.info("Allegiant: selected date via data-date")
                    return True
            except Exception:
                pass

            # Try calendar day buttons with exact day number
            cal_selectors = [
                "table td button", "table td a", "table td div",
                "[class*='calendar'] button", "[class*='calendar'] td",
                "[class*='datepicker'] button", "[class*='datepicker'] td",
                "[role='grid'] button", "[role='grid'] td",
                "[role='gridcell']", "[role='gridcell'] button",
            ]
            for sel in cal_selectors:
                try:
                    btn = page.locator(sel).filter(
                        has_text=re.compile(rf"^{day}$")
                    ).first
                    if await btn.count() > 0:
                        await btn.click(timeout=3000)
                        logger.info("Allegiant: selected day %d via '%s'", day, sel)
                        return True
                except Exception:
                    continue

            # Last resort: get_by_role button with day number
            try:
                btn = page.get_by_role("button", name=re.compile(rf"^{day}$")).first
                await btn.click(timeout=3000)
                logger.info("Allegiant: selected day %d via role button", day)
                return True
            except Exception:
                pass

            # gridcell role
            try:
                btn = page.get_by_role("gridcell", name=re.compile(rf"^{day}$")).first
                await btn.click(timeout=3000)
                return True
            except Exception:
                pass

            logger.warning("Allegiant: could not select day %d", day)
            return False

        except Exception as e:
            logger.warning("Allegiant: date error: %s", e)
            return False

    # ── Search button ────────────────────────────────────────────────────

    async def _click_search(self, page) -> None:
        """Click the search/submit button."""
        for label in [
            "Search", "SEARCH", "Search Flights", "Find Flights",
            "Find flights", "Search flights", "FIND FLIGHTS",
        ]:
            try:
                btn = page.get_by_role(
                    "button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)
                )
                if await btn.count() > 0:
                    await btn.first.click(timeout=5000)
                    return
            except Exception:
                continue
        # Link styled as button
        for label in ["Search", "Find Flights"]:
            try:
                link = page.get_by_role("link", name=re.compile(rf"{re.escape(label)}", re.IGNORECASE))
                if await link.count() > 0:
                    await link.first.click(timeout=5000)
                    return
            except Exception:
                continue
        try:
            await page.locator("button[type='submit']").first.click(timeout=3000)
        except Exception:
            try:
                await page.keyboard.press("Enter")
            except Exception:
                pass

    # ── DOM extraction fallback ──────────────────────────────────────────

    async def _extract_from_dom(self, page, req: FlightSearchRequest) -> list[FlightOffer]:
        """Extract flight data from page JS globals or script tags."""
        await asyncio.sleep(5)
        try:
            data = await page.evaluate("""() => {
                // React/Next.js globals
                if (window.__NEXT_DATA__) return window.__NEXT_DATA__;
                if (window.__NUXT__) return window.__NUXT__;
                // Allegiant may store search results in a global
                if (window.__APP_STATE__) return window.__APP_STATE__;
                if (window.__INITIAL_STATE__) return window.__INITIAL_STATE__;
                if (window.__PRELOADED_STATE__) return window.__PRELOADED_STATE__;
                // Check Redux store
                try {
                    const rootEl = document.getElementById('root') || document.getElementById('app');
                    if (rootEl && rootEl._reactRootContainer) {
                        const fiber = rootEl._reactRootContainer._internalRoot?.current;
                        if (fiber) return {_note: 'React fiber found but cannot extract state'};
                    }
                } catch {}
                // Check inline script tags for JSON data
                const scripts = document.querySelectorAll('script[type="application/json"], script:not([src])');
                for (const s of scripts) {
                    try {
                        const text = s.textContent || '';
                        if (text.length < 50) continue;
                        const d = JSON.parse(text);
                        if (d && typeof d === 'object') {
                            const str = JSON.stringify(d).toLowerCase();
                            if (str.includes('flight') || str.includes('fare') ||
                                str.includes('journey') || str.includes('availab'))
                                return d;
                        }
                    } catch {}
                }
                return null;
            }""")
            if data:
                offers = self._parse_response(data, req)
                if offers:
                    return offers
        except Exception:
            pass

        # DOM scraping fallback: look for flight result cards
        try:
            cards = await page.evaluate(r"""() => {
                const results = [];
                // Look for elements that look like flight cards
                const selectors = [
                    '[class*="flight-card"]', '[class*="flightCard"]',
                    '[class*="flight-result"]', '[class*="flightResult"]',
                    '[class*="trip-option"]', '[class*="tripOption"]',
                    '[class*="fare-card"]', '[class*="fareCard"]',
                    '[data-testid*="flight"]', '[data-testid*="fare"]',
                ];
                for (const sel of selectors) {
                    const els = document.querySelectorAll(sel);
                    if (els.length > 0) {
                        for (const el of els) {
                            const text = el.innerText || '';
                            // Extract price-like patterns
                            const priceMatch = text.match(/[$]\s*(\d+(?:[.]\d{2})?)/);
                            const timeMatch = text.match(/(\d{1,2}:\d{2}\s*(?:AM|PM)?)/gi);
                            if (priceMatch) {
                                results.push({
                                    price: parseFloat(priceMatch[1]),
                                    times: timeMatch || [],
                                    text: text.substring(0, 200),
                                });
                            }
                        }
                        break;
                    }
                }
                return results;
            }""")
            if cards:
                return self._parse_dom_cards(cards, req)
        except Exception:
            pass

        return []

    def _parse_dom_cards(
        self, cards: list[dict], req: FlightSearchRequest
    ) -> list[FlightOffer]:
        """Parse flight offers from DOM-scraped card data."""
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []
        for i, card in enumerate(cards):
            price = card.get("price")
            if not price or price <= 0:
                continue
            times = card.get("times", [])
            dep_time = self._parse_time(times[0] if times else "")
            arr_time = self._parse_time(times[1] if len(times) > 1 else "")

            dep_dt = datetime.combine(req.date_from, dep_time) if dep_time else datetime(2000, 1, 1)
            arr_dt = datetime.combine(req.date_from, arr_time) if arr_time else datetime(2000, 1, 1)

            seg = FlightSegment(
                airline="G4", airline_name="Allegiant",
                flight_no="", origin=req.origin, destination=req.destination,
                departure=dep_dt, arrival=arr_dt, cabin_class="M",
            )
            dur = int((arr_dt - dep_dt).total_seconds()) if arr_dt > dep_dt else 0
            route = FlightRoute(segments=[seg], total_duration_seconds=dur, stopovers=0)
            offers.append(FlightOffer(
                id=f"g4_dom_{i}_{hashlib.md5(str(card).encode()).hexdigest()[:8]}",
                price=round(price, 2), currency="USD",
                price_formatted=f"${price:.2f}",
                outbound=route, inbound=None,
                airlines=["Allegiant"], owner_airline="G4",
                booking_url=booking_url, is_locked=False,
                source="allegiant_direct", source_tier="free",
            ))
        return offers

    @staticmethod
    def _parse_time(s: str):
        """Parse a time string like '10:30 AM' into a time object."""
        if not s:
            return None
        from datetime import time as dt_time
        s = s.strip().upper()
        for fmt in ("%I:%M %p", "%I:%M%p", "%H:%M"):
            try:
                return datetime.strptime(s, fmt).time()
            except ValueError:
                continue
        return None

    # ── API response parsing ─────────────────────────────────────────────

    def _parse_response(self, data: Any, req: FlightSearchRequest) -> list[FlightOffer]:
        if isinstance(data, list):
            data = {"flights": data}
        booking_url = self._build_booking_url(req)
        offers: list[FlightOffer] = []

        # Try many possible locations for flight data
        flights_raw = (
            data.get("outboundFlights") or data.get("outbound")
            or data.get("journeys") or data.get("flights")
            or data.get("availability", {}).get("trips", [])
            or data.get("data", {}).get("flights", [])
            or data.get("data", {}).get("journeys", [])
            or data.get("lowFareAvailability", {}).get("outboundOptions", [])
            or data.get("flightList", []) or data.get("tripOptions", [])
            or data.get("props", {}).get("pageProps", {}).get("flights", [])
            or []
        )
        if isinstance(flights_raw, dict):
            flights_raw = (
                flights_raw.get("outbound", [])
                or flights_raw.get("journeys", [])
                or flights_raw.get("flights", [])
            )
        if not isinstance(flights_raw, list):
            flights_raw = []

        for flight in flights_raw:
            offer = self._parse_single_flight(flight, req, booking_url)
            if offer:
                offers.append(offer)
        return offers

    def _parse_single_flight(
        self, flight: dict, req: FlightSearchRequest, booking_url: str
    ) -> Optional[FlightOffer]:
        best_price = self._extract_best_price(flight)
        if best_price is None or best_price <= 0:
            return None

        segments_raw = (
            flight.get("segments") or flight.get("legs")
            or flight.get("flights") or []
        )
        segments: list[FlightSegment] = []
        if segments_raw and isinstance(segments_raw, list):
            for seg in segments_raw:
                segments.append(self._build_segment(seg, req.origin, req.destination))
        else:
            segments.append(self._build_segment(flight, req.origin, req.destination))

        total_dur = 0
        if segments and segments[0].departure and segments[-1].arrival:
            total_dur = int(
                (segments[-1].arrival - segments[0].departure).total_seconds()
            )

        route = FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )
        flight_key = (
            flight.get("journeyKey") or flight.get("id")
            or f"{flight.get('departureDate', '')}_{time.monotonic()}"
        )
        return FlightOffer(
            id=f"g4_{hashlib.md5(str(flight_key).encode()).hexdigest()[:12]}",
            price=round(best_price, 2),
            currency="USD",
            price_formatted=f"${best_price:.2f}",
            outbound=route,
            inbound=None,
            airlines=["Allegiant"],
            owner_airline="G4",
            booking_url=booking_url,
            is_locked=False,
            source="allegiant_direct",
            source_tier="free",
        )

    @staticmethod
    def _extract_best_price(flight: dict) -> Optional[float]:
        fares = (
            flight.get("fares") or flight.get("fareProducts")
            or flight.get("bundles") or flight.get("fareBundles") or []
        )
        best = float("inf")
        for fare in fares:
            if isinstance(fare, dict):
                for key in [
                    "price", "amount", "totalPrice", "basePrice",
                    "fareAmount", "totalAmount", "totalFare",
                ]:
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
        for key in [
            "price", "lowestFare", "totalPrice", "farePrice",
            "amount", "lowestPrice", "totalFare",
        ]:
            p = flight.get(key)
            if p is not None:
                try:
                    v = float(p) if not isinstance(p, dict) else float(p.get("amount", 0))
                    if 0 < v < best:
                        best = v
                except (TypeError, ValueError):
                    pass
        return best if best < float("inf") else None

    def _build_segment(
        self, seg: dict, default_origin: str, default_dest: str
    ) -> FlightSegment:
        dep_str = (
            seg.get("departureDateTime") or seg.get("departure")
            or seg.get("departureDate") or seg.get("std") or ""
        )
        arr_str = (
            seg.get("arrivalDateTime") or seg.get("arrival")
            or seg.get("arrivalDate") or seg.get("sta") or ""
        )
        flight_no = str(
            seg.get("flightNumber") or seg.get("flight_no")
            or seg.get("number") or ""
        ).replace(" ", "")
        origin = (
            seg.get("origin") or seg.get("departureStation")
            or seg.get("departureAirport") or default_origin
        )
        destination = (
            seg.get("destination") or seg.get("arrivalStation")
            or seg.get("arrivalAirport") or default_dest
        )
        carrier = seg.get("carrierCode") or seg.get("carrier") or seg.get("airline") or "G4"
        return FlightSegment(
            airline=carrier,
            airline_name="Allegiant",
            flight_no=flight_no,
            origin=origin,
            destination=destination,
            departure=self._parse_dt(dep_str),
            arrival=self._parse_dt(arr_str),
            cabin_class="M",
        )

    # ── Response builders ────────────────────────────────────────────────

    def _build_response(
        self, offers: list[FlightOffer], req: FlightSearchRequest, elapsed: float
    ) -> FlightSearchResponse:
        offers.sort(key=lambda o: o.price)
        logger.info(
            "Allegiant %s→%s returned %d offers in %.1fs",
            req.origin, req.destination, len(offers), elapsed,
        )
        h = hashlib.md5(
            f"allegiant{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
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
        for fmt in (
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M",
        ):
            try:
                return datetime.strptime(s[: len(fmt) + 2], fmt)
            except (ValueError, IndexError):
                continue
        return datetime(2000, 1, 1)

    @staticmethod
    def _build_booking_url(req: FlightSearchRequest) -> str:
        dep = req.date_from.strftime("%Y-%m-%d")
        return (
            f"https://www.allegiantair.com/flights?from={req.origin}"
            f"&to={req.destination}&departure={dep}"
            f"&adults={req.adults}&tripType=oneway"
        )

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(
            f"allegiant{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="USD",
            offers=[],
            total_results=0,
        )
