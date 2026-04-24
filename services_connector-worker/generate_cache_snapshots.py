#!/usr/bin/env python3
"""
Generate Chrome cache snapshots for LetsFG connector-worker.

This script visits airline websites in Chrome, lets them load their JS bundles,
then packages the Chrome disk cache as a tar.gz and uploads to GCS.

Run weekly via Cloud Build or manual trigger:
    python generate_cache_snapshots.py

Prerequisites:
    - Chrome installed
    - gcloud CLI authenticated
    - GCS bucket exists: gs://letsfg-chrome-cache/

The generated snapshots reduce proxy bandwidth by ~80% for browser connectors
by pre-populating Chrome's disk cache with JS bundles, CSS, etc.
"""

import argparse
import asyncio
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Airline URLs to snapshot — maps snapshot name to homepage URL
# These should match the _CONNECTOR_CACHE_SNAPSHOTS keys in main.py
AIRLINE_URLS: dict[str, str] = {
    # ── Already have GCS snapshots ──
    "ryanair": "https://www.ryanair.com/",
    "easyjet": "https://www.easyjet.com/",
    "wizzair": "https://wizzair.com/",
    "vueling": "https://www.vueling.com/",
    "pegasus": "https://www.flypgs.com/",
    "emirates": "https://www.emirates.com/",
    "spirit": "https://www.spirit.com/",
    "frontier": "https://www.flyfrontier.com/",
    "jetblue": "https://www.jetblue.com/",
    "southwest": "https://www.southwest.com/",
    "scoot": "https://www.flyscoot.com/",
    "vietjet": "https://www.vietjetair.com/",
    "airasia": "https://www.airasia.com/",
    "copa": "https://www.copaair.com/",
    "volaris": "https://www.volaris.com/",
    "suncountry": "https://www.suncountry.com/",
    "skyscanner": "https://www.skyscanner.com/",
    # ── Previously failed (<100KB) — retry in case of transient block ──
    "norwegian": "https://www.norwegian.com/",
    "transavia": "https://www.transavia.com/",
    "turkish": "https://www.turkishairlines.com/",
    "etihad": "https://www.etihad.com/",
    "aireuropa": "https://www.aireuropa.com/",
    "singapore": "https://www.singaporeair.com/",
    "jetstar": "https://www.jetstar.com/",
    "indigo": "https://www.goindigo.in/",
    "latam": "https://www.latamairlines.com/",
    "avianca": "https://www.avianca.com/",
    "allegiant": "https://www.allegiantair.com/",
    # ── CDP browser connectors — NEW ──
    "edreams": "https://www.edreams.com/",
    "opodo": "https://www.opodo.co.uk/",
    "smartwings": "https://www.smartwings.com/",
    "qatar": "https://www.qatarairways.com/",
    "eurowings": "https://www.eurowings.com/",
    "westjet": "https://www.westjet.com/",
    "cebupacific": "https://www.cebupacificair.com/",
    "lot": "https://www.lot.com/",
    "porter": "https://www.flyporter.com/",
    "jetsmart": "https://jetsmart.com/",
    "volotea": "https://www.volotea.com/",
    "finnair": "https://www.finnair.com/",
    "peach": "https://www.flypeach.com/",
    "itaairways": "https://www.ita-airways.com/",
    "korean": "https://www.koreanair.com/",
    "traveloka": "https://www.traveloka.com/",
    "saudia": "https://www.saudia.com/",
    "webjet": "https://www.webjet.com.au/",
    "tiket": "https://www.tiket.com/",
    "airchina": "https://www.airchina.com.cn/",
    "chinaeastern": "https://us.ceair.com/",
    "chinaairlines": "https://www.china-airlines.com/",
    "asiana": "https://flyasiana.com/",
    "airtransat": "https://www.airtransat.com/",
    "airserbia": "https://www.airserbia.com/",
    "mea": "https://www.mea.com.lb/",
    "hainan": "https://www.hainanairlines.com/",
    "level": "https://www.flylevel.com/",
    "transnusa": "https://book-transnusa.crane.aero/",
    "superairjet": "https://www.superairjet.com/",
    "citilink": "https://www.citilink.co.id/",
    "twayair": "https://www.twayair.com/",
    "virginatlantic": "https://www.virginatlantic.com/",
    "lufthansa": "https://www.lufthansa.com/",
    # ── Patchright browser connectors — NEW ──
    "american": "https://www.aa.com/",
    "delta": "https://www.delta.com/",
    "chinasouthern": "https://www.csair.com/",
    # ── Meta-search engines (highest value — each queries 100+ airlines) ──
    "momondo": "https://www.momondo.com/",
    "kayak": "https://www.kayak.com/",
    "cheapflights": "https://www.cheapflights.com/",
    "wego": "https://www.wego.com/",
    "aviasales": "https://www.aviasales.com/",
    "agoda": "https://www.agoda.com/flights",
    "ixigo": "https://www.ixigo.com/flights",
    "skiplagged": "https://skiplagged.com/",
    # ── OTAs (Online Travel Agencies) ──
    "tripcom": "https://www.trip.com/flights/",
    "bookingcom": "https://www.booking.com/flights/",
    "lastminute": "https://www.lastminute.com/flights",
    "travix": "https://www.budgetair.co.uk/",  # Travix brand
    "travelup": "https://www.travelup.com/",
    "byojet": "https://www.byojet.com/",
    "yatra": "https://www.yatra.com/flights",
    "auntbetty": "https://www.auntbetty.co.nz/",
    "flightcatchers": "https://www.flightcatchers.com/",
    "traveltrolley": "https://www.traveltrolley.co.uk/",
    "almosafer": "https://www.almosafer.com/en/flights",
    "musafir": "https://www.musafir.com/",
    "akbartravels": "https://www.akbartravels.com/flights",
    "airasiamove": "https://www.airasia.com/flights",
    "rehlat": "https://www.rehlat.com/",
    "travelstart": "https://www.travelstart.com/",
    "etraveli": "https://www.gotogate.com/",  # Etraveli brand
    "esky": "https://www.esky.com/",
    "despegar": "https://www.despegar.com/",
    "cleartrip": "https://www.cleartrip.com/flights",
    # ── Priority direct airlines (Gulf carriers, US Big 3, Asia full-service) ──
    "united": "https://www.united.com/",
    "nh": "https://www.ana.co.jp/",  # ANA
    "singapore": "https://www.singaporeair.com/",
    "korean": "https://www.koreanair.com/",
    "qatar": "https://www.qatarairways.com/",
    "etihad": "https://www.etihad.com/",
    "turkish": "https://www.turkishairlines.com/",
    "alaska": "https://www.alaskaair.com/",
    "hawaiian": "https://www.hawaiianairlines.com/",
    "bangkokairways": "https://www.bangkokair.com/",
    "philippineairlines": "https://www.philippineairlines.com/",
    "qantas": "https://www.qantas.com/",
    "airnewzealand": "https://www.airnewzealand.com/",
    # ── Additional browser connectors ──
    "airasiax": "https://www.airasia.com/flights",
    "aerolineas": "https://www.aerolineas.com.ar/",
    "azul": "https://www.voeazul.com.br/",
    "batikair": "https://www.batikair.com/",
    "breeze": "https://www.flybreeze.com/",
    "avelo": "https://www.aveloair.com/",
    "condor": "https://www.condor.com/",
    "flybondi": "https://www.flybondi.com/",
    "gol": "https://www.voegol.com.br/",
    "jet2": "https://www.jet2.com/",
    "norwegian": "https://www.norwegian.com/",
    "transavia": "https://www.transavia.com/",
    "zipair": "https://www.zipair.net/",
    "jejuair": "https://www.jejuair.net/",
    "nokair": "https://www.nokair.com/",
    "airpeace": "https://www.flyairpeace.com/",
    "usbangla": "https://www.usbangla.com/",
    "salamair": "https://www.salamair.com/",
    "biman": "https://www.bfrbd.com/",
    "flydubai": "https://www.flydubai.com/",
    "flynas": "https://www.flynas.com/",
    "sunexpress": "https://www.sunexpress.com/",
    "luckyair": "https://www.luckyair.net/",
    "9air": "https://www.fly9air.com/",
    "spring": "https://flights.ch.com/",
    "samoaairways": "https://www.samoaairways.com/",
    "solomonairlines": "https://www.flysolomons.com/",
    "skyairline": "https://www.skyairline.com/",
    "wingo": "https://www.wingo.com/",
    "kuwaitairways": "https://www.kuwaitairways.com/",
    "royaljordanian": "https://www.rj.com/",
    "mea": "https://www.mea.com.lb/",
}

GCS_BUCKET = os.environ.get("GCS_CACHE_BUCKET", "letsfg-chrome-cache")


async def create_snapshot(
    name: str,
    url: str,
    output_dir: Path,
    *,
    headless: bool = True,
    settle_wait_s: int = 8,
) -> Path | None:
    """
    Visit a URL in Chrome and save the disk cache as a tar.gz.
    
    Returns path to the created tar.gz, or None on failure.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("playwright not installed. Run: pip install playwright && playwright install chromium")
        return None

    user_data_dir = output_dir / f"chrome_{name}"
    user_data_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Creating snapshot for %s from %s", name, url)
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch_persistent_context(
                user_data_dir=str(user_data_dir),
                headless=headless,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-background-networking",
                    "--disable-component-update",
                    "--aggressive-cache-discard=false",
                    "--disable-http2",
                ],
                ignore_https_errors=True,
            )
            
            page = browser.pages[0] if browser.pages else await browser.new_page()
            
            # Navigate and wait for network to settle
            try:
                await page.goto(url, timeout=60000, wait_until="networkidle")
            except Exception as e:
                logger.warning("Navigation timeout for %s (expected for some SPAs): %s", name, e)
                # Still try to get what we can — partial cache is better than nothing
            
            # Give extra time for lazy-loaded JS bundles
            await asyncio.sleep(max(2, settle_wait_s))
            
            # Scroll to trigger lazy loading
            try:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                await asyncio.sleep(2)
            except Exception:
                pass
            
            await browser.close()
            
    except Exception as e:
        logger.error("Failed to create snapshot for %s: %s", name, e)
        return None

    # Package the cache directory
    tar_path = output_dir / f"{name}.tar.gz"
    
    try:
        with tarfile.open(tar_path, "w:gz") as tar:
            # Only include the Default directory (contains Cache, Code Cache, etc.)
            default_dir = user_data_dir / "Default"
            if default_dir.exists():
                for item in default_dir.iterdir():
                    # Include Cache, Code Cache, Service Worker, etc.
                    if item.name in ("Cache", "Code Cache", "GPUCache", "Service Worker"):
                        tar.add(item, arcname=f"Default/{item.name}")
            # Also check for cache_dir at top level (some Chromium versions)
            for item in user_data_dir.iterdir():
                if item.name in ("Cache", "Code Cache", "GPUCache") and item.is_dir():
                    tar.add(item, arcname=item.name)
        
        size_mb = tar_path.stat().st_size / 1024 / 1024
        logger.info("Created %s (%.1f MB)", tar_path.name, size_mb)
        
        # Cleanup user_data_dir
        shutil.rmtree(user_data_dir, ignore_errors=True)
        
        return tar_path
        
    except Exception as e:
        logger.error("Failed to package snapshot for %s: %s", name, e)
        return None


def upload_to_gcs(local_path: Path, bucket: str) -> bool:
    """Upload a file to GCS bucket using Python client."""
    try:
        from google.cloud import storage
    except ImportError:
        logger.error("google-cloud-storage not installed. Run: pip install google-cloud-storage")
        return False

    # Skip tiny snapshots (<100KB) — not worth caching
    size_kb = local_path.stat().st_size / 1024
    if size_kb < 100:
        logger.warning("Skipping %s — too small (%.0f KB), site likely blocked headless Chrome", local_path.name, size_kb)
        return False

    try:
        client = storage.Client()
        gcs_bucket = client.bucket(bucket)
        blob = gcs_bucket.blob(local_path.name)
        blob.upload_from_filename(str(local_path))
        logger.info("Uploaded %s to gs://%s/%s (%.1f KB)", local_path.name, bucket, local_path.name, size_kb)
        return True
    except Exception as e:
        logger.error("Upload failed for %s: %s", local_path.name, e)
        return False


async def main():
    parser = argparse.ArgumentParser(description="Generate Chrome cache snapshots for LetsFG")
    parser.add_argument("--airlines", nargs="+", help="Specific airlines to snapshot (default: all)")
    parser.add_argument("--output", default=None, help="Output directory (default: temp dir)")
    parser.add_argument("--no-upload", action="store_true", help="Skip GCS upload")
    parser.add_argument("--bucket", default=GCS_BUCKET, help=f"GCS bucket (default: {GCS_BUCKET})")
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Run visible Chrome instead of headless (helps on anti-bot sites)",
    )
    parser.add_argument(
        "--settle-wait",
        type=int,
        default=8,
        help="Seconds to wait after navigation for lazy-loaded assets (default: 8)",
    )
    parser.add_argument(
        "--per-airline-timeout",
        type=int,
        default=180,
        help="Hard timeout per airline snapshot in seconds (default: 180)",
    )
    args = parser.parse_args()

    # Determine which airlines to process
    airlines = args.airlines or list(AIRLINE_URLS.keys())
    invalid = set(airlines) - set(AIRLINE_URLS.keys())
    if invalid:
        logger.error("Unknown airlines: %s", invalid)
        logger.info("Available: %s", list(AIRLINE_URLS.keys()))
        return 1

    # Create output directory
    if args.output:
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        cleanup_output = False
    else:
        output_dir = Path(tempfile.mkdtemp(prefix="chrome_cache_"))
        cleanup_output = True

    logger.info("Output directory: %s", output_dir)
    logger.info("Processing %d airlines: %s", len(airlines), airlines)

    # Generate snapshots
    success_count = 0
    for name in airlines:
        url = AIRLINE_URLS[name]
        try:
            tar_path = await asyncio.wait_for(
                create_snapshot(
                    name,
                    url,
                    output_dir,
                    headless=not args.headed,
                    settle_wait_s=args.settle_wait,
                ),
                timeout=max(30, args.per_airline_timeout),
            )
        except asyncio.TimeoutError:
            logger.error(
                "Hard timeout (%ss) for %s — skipping and continuing",
                args.per_airline_timeout,
                name,
            )
            tar_path = None
        
        if tar_path and not args.no_upload:
            if upload_to_gcs(tar_path, args.bucket):
                success_count += 1
        elif tar_path:
            success_count += 1
            logger.info("Snapshot saved locally: %s", tar_path)

    logger.info("Completed: %d/%d snapshots", success_count, len(airlines))

    # Cleanup
    if cleanup_output:
        shutil.rmtree(output_dir, ignore_errors=True)

    return 0 if success_count == len(airlines) else 1


if __name__ == "__main__":
    exit(asyncio.run(main()))
