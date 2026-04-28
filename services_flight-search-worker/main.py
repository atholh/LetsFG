"""
LetsFG Flight Search Orchestrator — Cloud Run service for B2C DM-based flight search.

Fan-out architecture: fires N parallel HTTP calls to connector-worker instances.
Each connector runs on its own Cloud Run instance (174 connectors registered).
Route filtering ensures only ~15-40 relevant connectors fire per search.

Endpoints:
  POST /search      — Run two-phase flight search, callback with results
  POST /test-search — Direct search (returns results inline, no callback)
  GET  /health      — Health check

Two-phase search:
  Phase 1 (~3s):  LetsFG API backend — Duffel, Amadeus, Sabre (400+ airlines)
  Phase 2 (~60s): Fan-out to connector-worker — 174 connectors in parallel

Environment variables:
  WORKER_SECRET                  — Shared secret for authenticating inbound requests
  CALLBACK_SECRET                — Shared secret for callbacks to workflow engine
  LETSFG_API_KEY                 — API key for LetsFG backend (Phase 1). Optional.
  LETSFG_BASE_URL                — API base URL (default: https://api.letsfg.co)
  CONNECTOR_WORKER_URL           — URL of the connector-worker Cloud Run service
  CONNECTOR_WORKER_SECRET        — Shared secret for connector-worker authentication
    FANOUT_TIMEOUT                 — Max seconds to wait for the full fan-out.
                                                                     `0` disables the overall deadline (default).
    FANOUT_REQUEST_TIMEOUT         — Per-connector worker HTTP timeout (default: 240)
    FIRESTORE_PROJECT              — Firestore project ID (default: sms-caller)
    FIRESTORE_DATABASE             — Firestore database ID (default: boostedtravel)
  WEB_SEARCH_PERSIST_TTL_SECONDS — Persisted website search lifetime (default: 7200)
  WEB_SEARCH_FIRESTORE_COLLECTION — Firestore collection for persisted searches

Deploy (Cloud Run — NO --function flag):
  gcloud run deploy flight-search-worker \
    --source=. --project=sms-caller --region=us-central1 \
        --memory=512Mi --cpu=1 --concurrency=80 --max-instances=30 \
    --timeout=240 --min-instances=0 --cpu-throttling --no-traffic

    Cost notes:
    - concurrency=80: platform-level request cap; gunicorn stays single-process so in-memory search state remains coherent
    - max-instances=30: higher launch headroom for short-lived /web-search and /web-status traffic
        while keeping exact-search dedupe reasonably effective per instance
        - timeout=240: leaves headroom above long-running full-connector searches
  - cpu-throttling: CPU allocated only during request processing
  - min-instances=0: scales to zero when idle
"""

import asyncio
import hmac
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from urllib import error as urllib_error, request as urllib_request

from flask import Flask, abort, jsonify, request

try:
    from google.cloud import firestore
except ImportError:  # pragma: no cover - optional in local dev until deps are installed
    firestore = None

from search_worker import normalize_offer_currencies, run_search

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("worker")

app = Flask(__name__)

WORKER_SECRET = os.environ.get("WORKER_SECRET", "")
LETSFG_BASE_URL = os.environ.get("LETSFG_BASE_URL", "https://api.letsfg.co").rstrip("/")
FIRESTORE_PROJECT = os.environ.get("FIRESTORE_PROJECT", "sms-caller")
FIRESTORE_DATABASE = os.environ.get("FIRESTORE_DATABASE", "boostedtravel")
WEB_SEARCH_TTL_SECONDS = int(os.environ.get("WEB_SEARCH_TTL_SECONDS", "1800"))
WEB_SEARCH_CACHE_TTL_SECONDS = int(os.environ.get("WEB_SEARCH_CACHE_TTL_SECONDS", "240"))
WEB_SEARCH_PERSIST_TTL_SECONDS = int(os.environ.get("WEB_SEARCH_PERSIST_TTL_SECONDS", "7200"))
WEB_SEARCH_FIRESTORE_COLLECTION = os.environ.get("WEB_SEARCH_FIRESTORE_COLLECTION", "web_search_results")
PUBLIC_STATS_RECORD_SEARCH_URL = f"{LETSFG_BASE_URL}/api/v1/analytics/stats/record-search" if LETSFG_BASE_URL else ""
SEARCH_SESSION_UPSERT_URL = f"{LETSFG_BASE_URL}/api/v1/analytics/search-sessions/upsert" if LETSFG_BASE_URL else ""
GOOGLE_FLIGHTS_SOURCES = {"serpapi_google"}

# In-memory store for web-facing searches: { search_id → { status, offers, started_at } }
# gunicorn runs with --workers 1 so this is safe across threads.
_web_searches: dict = {}
_web_search_cache: dict = {}
_web_search_inflight: dict = {}
_web_searches_lock = threading.Lock()
_firestore_client = None
_firestore_init_failed = False
_firestore_lock = threading.Lock()


def _verify_auth():
    """Verify inbound request has the correct shared secret."""
    if not WORKER_SECRET:
        return  # No auth in local dev
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    if not token or not hmac.compare_digest(token, WORKER_SECRET):
        abort(401, "Unauthorized")


def _new_web_search_id() -> str:
    return f"ws_{uuid.uuid4().hex}"


def _normalize_web_search_params(
    origin: str,
    destination: str,
    date_from: str,
    return_date: str | None,
    adults: int,
    currency: str,
    max_stops: int | None,
    cabin_class: str | None,
) -> dict:
    return {
        "origin": origin.strip().upper(),
        "destination": destination.strip().upper(),
        "date_from": date_from.strip(),
        "return_date": return_date or None,
        "adults": max(1, int(adults or 1)),
        "currency": (currency or "EUR").strip().upper(),
        "max_stops": int(max_stops) if max_stops is not None else None,
        "cabin": cabin_class or None,
    }


def _build_web_search_cache_key(
    origin: str,
    destination: str,
    date_from: str,
    return_date: str | None,
    adults: int,
    currency: str,
    max_stops: int | None,
    cabin_class: str | None,
) -> str:
    normalized = _normalize_web_search_params(
        origin,
        destination,
        date_from,
        return_date,
        adults,
        currency,
        max_stops,
        cabin_class,
    )
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def _make_web_search_entry(
    status: str,
    started_at: float,
    origin: str,
    destination: str,
    date_from: str,
    return_date: str | None,
    offers: list | None = None,
    elapsed_seconds: float | None = None,
    savings_vs_google_flights_usd: float | None = None,
    error: str | None = None,
    expires_at: float | None = None,
) -> dict:
    entry = {
        "status": status,
        "offers": offers or [],
        "started_at": started_at,
        "expires_at": expires_at or (started_at + WEB_SEARCH_TTL_SECONDS),
        "origin": origin,
        "destination": destination,
        "date_from": date_from,
        "return_date": return_date,
    }
    if elapsed_seconds is not None:
        entry["elapsed_seconds"] = elapsed_seconds
    if savings_vs_google_flights_usd is not None:
        entry["savings_vs_google_flights_usd"] = savings_vs_google_flights_usd
    if error:
        entry["error"] = error
    return entry


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp_to_epoch(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.timestamp()
    timestamp = getattr(value, "timestamp", None)
    if callable(timestamp):
        try:
            return float(timestamp())
        except Exception:
            return None
    return None


def _get_firestore_client():
    global _firestore_client, _firestore_init_failed

    if WEB_SEARCH_PERSIST_TTL_SECONDS <= 0 or firestore is None:
        return None
    if _firestore_client is not None:
        return _firestore_client
    if _firestore_init_failed:
        return None

    with _firestore_lock:
        if _firestore_client is not None:
            return _firestore_client
        if _firestore_init_failed:
            return None
        try:
            _firestore_client = firestore.Client(
                project=FIRESTORE_PROJECT,
                database=FIRESTORE_DATABASE,
            )
        except Exception as exc:
            logger.warning("web-search persistence unavailable: %s", exc)
            _firestore_init_failed = True
            return None

    return _firestore_client


def _build_persisted_web_search_doc(search_id: str, entry: dict, now_dt: datetime | None = None) -> dict:
    now_dt = now_dt or _utc_now()
    return {
        "search_id": search_id,
        "status": entry.get("status"),
        "origin": entry.get("origin"),
        "destination": entry.get("destination"),
        "date_from": entry.get("date_from"),
        "return_date": entry.get("return_date"),
        "offers": list(entry.get("offers", [])),
        "elapsed_seconds": entry.get("elapsed_seconds"),
        "savings_vs_google_flights_usd": entry.get("savings_vs_google_flights_usd"),
        "started_at": datetime.fromtimestamp(entry.get("started_at", time.time()), timezone.utc),
        "stored_at": now_dt,
        "expires_at": now_dt + timedelta(seconds=WEB_SEARCH_PERSIST_TTL_SECONDS),
    }


def _persist_web_search_results(entries: list[tuple[str, dict]]) -> None:
    if not entries:
        return

    client = _get_firestore_client()
    if client is None:
        return

    collection = client.collection(WEB_SEARCH_FIRESTORE_COLLECTION)
    batch = client.batch()
    now_dt = _utc_now()
    writes = 0

    for search_id, entry in entries:
        if not search_id or entry.get("status") != "completed":
            continue
        batch.set(
            collection.document(search_id),
            _build_persisted_web_search_doc(search_id, entry, now_dt),
            merge=True,
        )
        writes += 1

    if writes == 0:
        return

    try:
        batch.commit()
    except Exception as exc:
        logger.warning("failed to persist web searches: %s", exc)


def _persist_web_search_results_async(entries: list[tuple[str, dict]]) -> None:
    if not entries:
        return

    snapshot = [(search_id, dict(entry)) for search_id, entry in entries]
    threading.Thread(
        target=_persist_web_search_results,
        args=(snapshot,),
        daemon=True,
    ).start()


def _load_persisted_web_search(search_id: str, now: float | None = None) -> dict | None:
    client = _get_firestore_client()
    if client is None:
        return None

    now = now or time.time()
    try:
        snap = client.collection(WEB_SEARCH_FIRESTORE_COLLECTION).document(search_id).get()
    except Exception as exc:
        logger.warning("failed to load persisted web search %s: %s", search_id, exc)
        return None

    if not snap.exists:
        return None

    data = snap.to_dict() or {}
    if data.get("status") != "completed":
        return None

    expires_at = _timestamp_to_epoch(data.get("expires_at"))
    if expires_at is not None and now >= expires_at:
        try:
            snap.reference.delete()
        except Exception:
            pass
        return None

    started_at = _timestamp_to_epoch(data.get("started_at")) or now
    savings_vs_google_flights_usd = data.get("savings_vs_google_flights_usd")
    if savings_vs_google_flights_usd is None:
        savings_vs_google_flights_usd = _compute_savings_vs_google_flights(list(data.get("offers") or []))
    return _make_web_search_entry(
        "completed",
        started_at,
        data.get("origin") or "",
        data.get("destination") or "",
        data.get("date_from") or "",
        data.get("return_date"),
        offers=list(data.get("offers") or []),
        elapsed_seconds=data.get("elapsed_seconds"),
        savings_vs_google_flights_usd=savings_vs_google_flights_usd,
        expires_at=expires_at or (now + WEB_SEARCH_PERSIST_TTL_SECONDS),
    )


def _prune_web_search_state(now: float | None = None) -> None:
    now = now or time.time()

    stale_search_ids = []
    for search_id, entry in _web_searches.items():
        expires_at = entry.get("expires_at") or (entry.get("started_at", now) + WEB_SEARCH_TTL_SECONDS)
        if now >= expires_at:
            stale_search_ids.append(search_id)
    for search_id in stale_search_ids:
        del _web_searches[search_id]

    stale_cache_keys = [
        cache_key
        for cache_key, entry in _web_search_cache.items()
        if now >= entry.get("expires_at", 0)
    ]
    for cache_key in stale_cache_keys:
        del _web_search_cache[cache_key]

    stale_inflight_keys = []
    for cache_key, group in _web_search_inflight.items():
        live_search_ids = {search_id for search_id in group.get("search_ids", set()) if search_id in _web_searches}
        if live_search_ids:
            group["search_ids"] = live_search_ids
        else:
            stale_inflight_keys.append(cache_key)
    for cache_key in stale_inflight_keys:
        del _web_search_inflight[cache_key]


def _offer_price_value(offer: dict) -> float | None:
    for candidate in (offer.get("price"), offer.get("price_normalized")):
        try:
            value = float(candidate)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def _compute_google_flights_comparison(offers: list[dict]) -> dict[str, float | None]:
    cheapest_overall = None
    cheapest_google = None

    for offer in offers or []:
        price = _offer_price_value(offer)
        if price is None:
            continue
        if cheapest_overall is None or price < cheapest_overall:
            cheapest_overall = price
        if str(offer.get("source") or "").lower() in GOOGLE_FLIGHTS_SOURCES:
            if cheapest_google is None or price < cheapest_google:
                cheapest_google = price

    value = None
    savings_vs_google = None
    if cheapest_overall is not None and cheapest_google is not None:
        diff = round(cheapest_google - cheapest_overall, 2)
        value = 0.0 if abs(diff) < 0.005 else diff
        savings_vs_google = round(max(0.0, diff), 2)

    return {
        "cheapest_price": cheapest_overall,
        "google_flights_price": cheapest_google,
        "value": value,
        "savings_vs_google_flights": savings_vs_google,
    }


def _compute_savings_vs_google_flights(offers: list[dict]) -> float | None:
    comparison = _compute_google_flights_comparison(offers)
    return comparison["savings_vs_google_flights"]


def _build_worker_search_session_payload(search_id: str, entry: dict, event_type: str, cache_hit: bool = False) -> dict:
    offers = list(entry.get("offers") or [])
    comparison = _compute_google_flights_comparison(offers)
    started_at = datetime.fromtimestamp(entry.get("started_at", time.time()), timezone.utc).isoformat()
    completed_at = _utc_now().isoformat()
    results_count = len(offers)
    value = comparison["value"]
    if value is None and (entry.get("status") == "failed" or results_count == 0):
        value = -100.0

    payload = {
        "search_id": search_id,
        "source": "flight-search-worker",
        "status": entry.get("status"),
        "origin": entry.get("origin"),
        "destination": entry.get("destination"),
        "route": f"{entry.get('origin')}-{entry.get('destination')}" if entry.get("origin") and entry.get("destination") else None,
        "date_from": entry.get("date_from"),
        "return_date": entry.get("return_date"),
        "search_started_at": started_at,
        "search_completed_at": completed_at,
        "search_duration_seconds": entry.get("elapsed_seconds"),
        "results_count": results_count,
        "cheapest_price": comparison["cheapest_price"],
        "google_flights_price": comparison["google_flights_price"],
        "value": value,
        "savings_vs_google_flights": comparison["savings_vs_google_flights"],
        "cache_hit": cache_hit or None,
        "cost_per_search": 0.0,
        "other_costs": 0.0,
        "event": {
            "type": event_type,
            "at": completed_at,
            "data": {
                "results_count": results_count,
                "cache_hit": cache_hit,
                "error": entry.get("error"),
            },
        },
    }

    if entry.get("status") == "failed":
        payload["decision"] = "search_failed"

    return {key: value for key, value in payload.items() if value is not None}

    return round(max(0.0, cheapest_google - cheapest_overall), 2)


def _record_public_search_stats(entries: list[tuple[str, dict]]) -> None:
    if not PUBLIC_STATS_RECORD_SEARCH_URL or not entries:
        return

    for search_id, entry in entries:
        if not search_id or entry.get("status") != "completed":
            continue

        payload = json.dumps({
            "savings_usd": entry.get("savings_vs_google_flights_usd"),
        }).encode("utf-8")
        request_obj = urllib_request.Request(
            PUBLIC_STATS_RECORD_SEARCH_URL,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Origin": "https://letsfg.co",
                "Referer": "https://letsfg.co/",
                "User-Agent": "Mozilla/5.0 (compatible; LetsFG Website Search/1.0; +https://letsfg.co)",
            },
            method="POST",
        )

        try:
            with urllib_request.urlopen(request_obj, timeout=5) as response:
                if getattr(response, "status", 200) >= 400:
                    logger.warning(
                        "failed to record public search stats for %s: status=%s",
                        search_id,
                        getattr(response, "status", "unknown"),
                    )
        except urllib_error.HTTPError as exc:
            logger.warning("failed to record public search stats for %s: %s", search_id, exc)
        except Exception as exc:
            logger.warning("failed to record public search stats for %s: %s", search_id, exc)


def _record_search_sessions(entries: list[tuple[str, dict]], event_type: str, cache_hit: bool = False) -> None:
    if not SEARCH_SESSION_UPSERT_URL or not entries:
        return

    for search_id, entry in entries:
        if not search_id:
            continue

        payload = _build_worker_search_session_payload(search_id, entry, event_type, cache_hit=cache_hit)
        request_obj = urllib_request.Request(
            SEARCH_SESSION_UPSERT_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Origin": "https://letsfg.co",
                "Referer": "https://letsfg.co/",
                "User-Agent": "Mozilla/5.0 (compatible; LetsFG Worker/1.0; +https://letsfg.co)",
            },
            method="POST",
        )

        try:
            with urllib_request.urlopen(request_obj, timeout=5) as response:
                if getattr(response, "status", 200) >= 400:
                    logger.warning(
                        "failed to record search session for %s: status=%s",
                        search_id,
                        getattr(response, "status", "unknown"),
                    )
        except urllib_error.HTTPError as exc:
            logger.warning("failed to record search session for %s: %s", search_id, exc)
        except Exception as exc:
            logger.warning("failed to record search session for %s: %s", search_id, exc)


def _record_public_search_stats_async(entries: list[tuple[str, dict]]) -> None:
    if not entries:
        return

    snapshot = [(search_id, dict(entry)) for search_id, entry in entries]
    threading.Thread(
        target=_record_public_search_stats,
        args=(snapshot,),
        daemon=True,
    ).start()


def _record_search_sessions_async(entries: list[tuple[str, dict]], event_type: str, cache_hit: bool = False) -> None:
    if not entries:
        return

    snapshot = [(search_id, dict(entry)) for search_id, entry in entries]
    threading.Thread(
        target=_record_search_sessions,
        args=(snapshot, event_type, cache_hit),
        daemon=True,
    ).start()


@app.route("/search", methods=["POST"])
def search():
    """
    Run two-phase flight search and callback with results.

    Expected JSON body:
      {
        "origin": "LON",
        "destination": "BCN",
        "date_from": "2026-04-15",
        "return_date": "2026-04-22",
        "callback_url": "https://workflow-engine-xxx.run.app/flight-search-callback",
        "callback_meta": {"companyId": "...", "leadId": "...", "instanceId": "..."},
        "adults": 1,
        "currency": "EUR"
      }

    When return_date is present, outbound + return are searched in parallel
    and cross-airline combos are built via virtual interlining.
    """
    _verify_auth()

    data = request.get_json(force=True)

    required = ["origin", "destination", "date_from", "callback_url", "callback_meta"]
    missing = [f for f in required if f not in data]
    if missing:
        return jsonify({"error": f"Missing: {', '.join(missing)}"}), 400

    try:
        result = asyncio.run(run_search(data))
        return jsonify({
            "status": "completed",
            "total_offers": result.get("total_results", 0),
        })
    except Exception as exc:
        logger.exception("Search failed: %s", exc)
        return jsonify({"status": "error", "error": str(exc)}), 500


@app.route("/test-search", methods=["POST"])
def test_search():
    """
    Direct search endpoint — returns results inline (no callback).

    Runs both API backend (Duffel/Amadeus/Sabre — 400+ airlines) and
    fan-out to connector-worker instances in parallel, then merges.

    Body: {
      "origin": "LON", "destination": "DEL", "date_from": "2026-04-01",
      "return_date": "2026-04-07",  // optional
      "adults": 1, "currency": "GBP", "limit": 20, "max_stops": 0
    }
    """
    _verify_auth()
    data = request.get_json(force=True)

    origin = (data.get("origin") or "").strip().upper()
    destination = (data.get("destination") or "").strip().upper()
    date_from = (data.get("date_from") or "").strip()

    if not origin or not destination or not date_from:
        return jsonify({"error": "Need origin, destination, date_from"}), 400

    adults = int(data.get("adults", 1))
    currency = data.get("currency", "EUR")
    limit = int(data.get("limit", 20))
    return_date = (data.get("return_date") or "").strip() or None
    max_stops = data.get("max_stops")
    if max_stops is not None:
        max_stops = int(max_stops)
    cabin_class = (data.get("cabin_class") or "").strip().upper() or None
    if cabin_class and cabin_class not in ("M", "W", "C", "F"):
        cabin_class = None  # ignore invalid values

    try:
        result = asyncio.run(_run_test_search(
            origin, destination, date_from, return_date,
            adults, currency, limit, max_stops, cabin_class,
        ))
        return jsonify(result)
    except Exception as exc:
        logger.exception("Test search failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


async def _run_test_search(
    origin: str, destination: str, date_from: str, return_date: str | None,
    adults: int, currency: str, limit: int, max_stops: int | None,
    cabin_class: str | None = None,
) -> dict:
    """Run API + local search in parallel, merge results.

    For round-trip:
      - API gets ONE call with return_from → native round-trip offers
        (airlines price outbound+return together = much cheaper)
      - Only non-RT-capable local connectors feed the combo engine as
        separate outbound/return legs
      - RT-capable connectors get their own dedicated round-trip fan-out
      - Both streams are merged and sorted by price
    """
    import time as _time
    from search_worker import (
        RT_CAPABLE_CONNECTORS,
        _deduplicate,
        _filter_by_stops,
        _filter_route_mismatch,
        _get_valid_airports,
        _search_api,
        _search_local,
    )

    LETSFG_API_KEY = os.environ.get("LETSFG_API_KEY", "")
    t0 = _time.monotonic()

    api_tasks = []
    api_labels = []
    local_tasks = []
    local_labels = []

    # API search (Duffel/Amadeus/Sabre) — native round-trip when return_date set
    if LETSFG_API_KEY:
        api_tasks.append(_search_api(
            origin, destination, date_from, adults, currency, limit * 2,
            max_stopovers=max_stops, return_date=return_date,
            cabin_class=cabin_class,
        ))
        api_labels.append("api_rt" if return_date else "api_out")

    # Local connector fan-out — non-RT-capable legs for the combo engine
    combo_leg_excludes = RT_CAPABLE_CONNECTORS if return_date else None
    local_tasks.append(_search_local(
        origin, destination, date_from, adults, currency, limit * 2,
        cabin_class=cabin_class, exclude_connectors=combo_leg_excludes,
    ))
    local_labels.append("local_out")
    if return_date:
        local_tasks.append(_search_local(
            destination, origin, return_date, adults, currency, limit * 2,
            cabin_class=cabin_class, exclude_connectors=combo_leg_excludes,
        ))
        local_labels.append("local_ret")
        # RT-capable connectors return complete itineraries in one worker call.
        local_tasks.append(_search_local(
            origin, destination, date_from, adults, currency, limit * 2,
            return_date=return_date, only_rt_capable=True,
            cabin_class=cabin_class,
        ))
        local_labels.append("local_rt")

    # Start all tasks
    all_task_objs = [asyncio.ensure_future(t) for t in api_tasks + local_tasks]
    api_task_objs = all_task_objs[:len(api_tasks)]
    local_task_objs = all_task_objs[len(api_tasks):]

    # Let the API/local search tasks finish on their own deadlines.
    # _search_api and _search_local already enforce their own timeouts, so
    # adding a second outer timeout here truncates web searches early.
    await asyncio.gather(*all_task_objs, return_exceptions=True)

    # Collect results by source
    task_labels = api_labels + local_labels
    api_offers = []       # native round-trip offers from API (already have outbound+inbound)
    local_out = []        # one-way outbound from local connectors
    local_ret = []        # one-way return from local connectors
    local_rt = []         # native round-trip from aggregators (Skyscanner, Kayak, etc.)
    one_way_offers = []   # all offers when no return_date

    for label, task_obj in zip(task_labels, all_task_objs):
        if task_obj.done() and not task_obj.cancelled():
            try:
                res = task_obj.result()
            except Exception as exc:
                logger.error("test-search %s failed: %s", label, exc)
                continue
            if isinstance(res, dict):
                offers = res.get("offers", [])
                logger.info("test-search %s: %d offers", label, len(offers))
                if label == "api_rt":
                    api_offers.extend(offers)
                elif label == "local_out":
                    local_out.extend(offers)
                elif label == "local_ret":
                    local_ret.extend(offers)
                elif label == "local_rt":
                    local_rt.extend(offers)
                else:
                    one_way_offers.extend(offers)
        else:
            logger.info("test-search %s: skipped (timeout/cancelled)", label)

    # Merge results
    if return_date:
        all_offers = list(api_offers) + list(local_rt)  # native round-trip offers

        # Build combos from local one-way legs using the SDK's combo engine
        # (virtual interlining — pairs outbound airline A with return airline B)
        if local_out and local_ret:
            from search_worker import _build_round_trip_combos, _filter_by_stops

            combo_out = local_out
            combo_ret = local_ret
            if max_stops is not None:
                combo_out = _filter_by_stops(local_out, max_stops)
                combo_ret = _filter_by_stops(local_ret, max_stops)
                logger.info("test-search pre-filtered for combos: %d out, %d ret",
                            len(combo_out), len(combo_ret))

            combos = _build_round_trip_combos(combo_out, combo_ret, currency, max_combos=limit)
            logger.info("test-search local combos: %d", len(combos))
            all_offers.extend(combos)

        logger.info("test-search round-trip: %d API native + %d agg RT + %d local combos = %d total",
                     len(api_offers), len(local_rt),
                     len(all_offers) - len(api_offers) - len(local_rt), len(all_offers))
    else:
        all_offers = one_way_offers + local_out

    merged = _deduplicate(all_offers)
    # Route validation — drop offers whose origin/destination don't match the request
    valid_origins, valid_dests = _get_valid_airports(origin, destination)
    merged = _filter_route_mismatch(merged, valid_origins, valid_dests)
    if max_stops is not None:
        merged = _filter_by_stops(merged, max_stops)

    # Normalize all prices to the requested currency before sorting.
    # Connectors return prices in their native currencies (USD, EUR, PLN, etc.)
    # — sorting raw price numbers across currencies is meaningless.
    normalize_offer_currencies(merged, currency)

    # Rank: price + 8% penalty per stop (direct flights rank higher)
    merged.sort(key=lambda o: float(o.get("price", 999999)) * (
        1 + 0.08 * ((o.get("outbound") or {}).get("stopovers", 0)
                    + (o.get("inbound") or {}).get("stopovers", 0))))
    merged = merged[:limit]

    elapsed = _time.monotonic() - t0
    return {
        "offers": merged,
        "total_results": len(merged),
        "elapsed_seconds": round(elapsed, 1),
    }


@app.route("/web-search", methods=["POST"])
def web_search():
    """
    Start a website-facing flight search. Returns { search_id } immediately.
    Poll GET /web-status/<search_id> for results.

    Body: { origin, destination, date_from, return_date?, adults?, currency?,
            limit?, max_stops?, cabin? }
    """
    _verify_auth()
    data = request.get_json(force=True)

    origin = (data.get("origin") or "").strip().upper()
    destination = (data.get("destination") or "").strip().upper()
    date_from = (data.get("date_from") or "").strip()

    if not origin or not destination or not date_from:
        return jsonify({"error": "Need origin, destination, date_from"}), 400

    adults = int(data.get("adults", 1))
    currency = (data.get("currency") or "EUR").upper()
    limit = int(data.get("limit", 500))
    return_date = (data.get("return_date") or "").strip() or None
    max_stops = data.get("max_stops")
    if max_stops is not None:
        max_stops = int(max_stops)
    cabin_class = (data.get("cabin") or "").strip().upper() or None
    if cabin_class and cabin_class not in ("M", "W", "C", "F"):
        cabin_class = None

    cache_key = _build_web_search_cache_key(
        origin,
        destination,
        date_from,
        return_date,
        adults,
        currency,
        max_stops,
        cabin_class,
    )
    search_id = _new_web_search_id()
    now = time.time()
    should_start_search = False
    joined_inflight = False
    cache_hit = False
    cache_hit_entry = None

    with _web_searches_lock:
        _prune_web_search_state(now)

        cached_entry = _web_search_cache.get(cache_key)
        if cached_entry and now < cached_entry.get("expires_at", 0):
            cache_hit = True
            cached_savings = cached_entry.get("savings_vs_google_flights_usd")
            if cached_savings is None:
                cached_savings = _compute_savings_vs_google_flights(list(cached_entry.get("offers", [])))
            _web_searches[search_id] = _make_web_search_entry(
                "completed",
                now,
                origin,
                destination,
                date_from,
                return_date,
                offers=list(cached_entry.get("offers", [])),
                elapsed_seconds=cached_entry.get("elapsed_seconds"),
                savings_vs_google_flights_usd=cached_savings,
                expires_at=now + WEB_SEARCH_TTL_SECONDS,
            )
            cache_hit_entry = dict(_web_searches[search_id])
        else:
            _web_searches[search_id] = _make_web_search_entry(
                "searching",
                now,
                origin,
                destination,
                date_from,
                return_date,
            )

            inflight_group = _web_search_inflight.get(cache_key)
            if inflight_group:
                inflight_group["search_ids"].add(search_id)
                joined_inflight = True
            else:
                _web_search_inflight[cache_key] = {
                    "search_ids": {search_id},
                    "started_at": now,
                }
                should_start_search = True

    if cache_hit:
        _persist_web_search_results_async([(search_id, cache_hit_entry)])
        _record_public_search_stats_async([(search_id, cache_hit_entry)])
        _record_search_sessions_async([(search_id, cache_hit_entry)], "search_cache_hit", cache_hit=True)
        logger.info("web-search cache hit: %s → %s %s sid=%s", origin, destination, date_from, search_id)
        return jsonify({"search_id": search_id, "cache_hit": True})

    if joined_inflight:
        logger.info("web-search joined inflight: %s → %s %s sid=%s", origin, destination, date_from, search_id)
        return jsonify({"search_id": search_id, "cache_hit": False})

    def _run_in_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(_run_test_search(
                origin, destination, date_from, return_date,
                adults, currency, limit, max_stops, cabin_class,
            ))
            offers = result.get("offers", [])
            elapsed_seconds = result.get("elapsed_seconds")
            savings_vs_google_flights_usd = _compute_savings_vs_google_flights(offers)
            completed_at = time.time()
            persisted_entries = []
            with _web_searches_lock:
                _prune_web_search_state(completed_at)
                _web_search_cache[cache_key] = {
                    "offers": offers,
                    "elapsed_seconds": elapsed_seconds,
                    "savings_vs_google_flights_usd": savings_vs_google_flights_usd,
                    "expires_at": completed_at + WEB_SEARCH_CACHE_TTL_SECONDS,
                }
                inflight_group = _web_search_inflight.pop(cache_key, None)
                search_ids = list((inflight_group or {}).get("search_ids", {search_id}))
                for pending_search_id in search_ids:
                    if pending_search_id not in _web_searches:
                        continue
                    _web_searches[pending_search_id].update({
                        "status": "completed",
                        "offers": offers,
                        "elapsed_seconds": elapsed_seconds,
                        "savings_vs_google_flights_usd": savings_vs_google_flights_usd,
                        "expires_at": completed_at + WEB_SEARCH_TTL_SECONDS,
                    })
                    persisted_entries.append((pending_search_id, dict(_web_searches[pending_search_id])))

            _persist_web_search_results(persisted_entries)
            _record_public_search_stats_async(persisted_entries)
            _record_search_sessions_async(persisted_entries, "search_completed")
        except Exception as exc:
            logger.exception("web-search %s failed: %s", search_id, exc)
            with _web_searches_lock:
                failed_at = time.time()
                inflight_group = _web_search_inflight.pop(cache_key, None)
                search_ids = list((inflight_group or {}).get("search_ids", {search_id}))
                failed_entries = []
                for pending_search_id in search_ids:
                    if pending_search_id not in _web_searches:
                        continue
                    _web_searches[pending_search_id].update({
                        "status": "failed",
                        "error": str(exc),
                        "elapsed_seconds": round(failed_at - _web_searches[pending_search_id].get("started_at", failed_at), 1),
                        "expires_at": failed_at + WEB_SEARCH_TTL_SECONDS,
                    })
                    failed_entries.append((pending_search_id, dict(_web_searches[pending_search_id])))
            _record_search_sessions_async(failed_entries, "search_failed")
        finally:
            loop.close()

    if should_start_search:
        threading.Thread(target=_run_in_thread, daemon=True).start()

    logger.info("web-search started: %s → %s %s sid=%s", origin, destination, date_from, search_id)
    return jsonify({"search_id": search_id, "cache_hit": False})


@app.route("/web-status/<search_id>", methods=["GET"])
def web_status(search_id: str):
    """
    Poll the status of a web-facing search started via POST /web-search.
    Returns { status: "searching" | "completed" | "failed" | "not_found", offers?: [] }
    """
    _verify_auth()

    now = time.time()

    with _web_searches_lock:
        _prune_web_search_state(now)
        entry = _web_searches.get(search_id)

    if not entry:
        entry = _load_persisted_web_search(search_id, now)

    if not entry:
        return jsonify({"status": "not_found"}), 404

    expires_in_seconds = max(0, int((entry.get("expires_at") or now) - now))

    if entry["status"] == "completed":
        return jsonify({
            "status": "completed",
            "offers": entry.get("offers", []),
            "origin": entry.get("origin"),
            "destination": entry.get("destination"),
            "date_from": entry.get("date_from"),
            "return_date": entry.get("return_date"),
            "elapsed_seconds": entry.get("elapsed_seconds"),
            "savings_vs_google_flights_usd": entry.get("savings_vs_google_flights_usd"),
            "expires_in_seconds": expires_in_seconds,
        })

    if entry["status"] == "failed":
        return jsonify({
            "status": "failed",
            "error": entry.get("error"),
            "origin": entry.get("origin"),
            "destination": entry.get("destination"),
            "date_from": entry.get("date_from"),
            "return_date": entry.get("return_date"),
            "expires_in_seconds": expires_in_seconds,
        })

    return jsonify({
        "status": entry["status"],
        "origin": entry.get("origin"),
        "destination": entry.get("destination"),
        "date_from": entry.get("date_from"),
        "return_date": entry.get("return_date"),
        "expires_in_seconds": expires_in_seconds,
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "service": "flight-search-worker"})
