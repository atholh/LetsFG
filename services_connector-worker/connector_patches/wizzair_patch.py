import asyncio
import functools
import hashlib
import logging
import time

from letsfg.connectors import wizzair as _wizzair_module
from letsfg.connectors.wizzair import WizzairConnectorClient as _BaseWizzairConnectorClient
from letsfg.models.flights import FlightSearchRequest, FlightSearchResponse

logger = logging.getLogger(__name__)


def _search_timetable_sync_public(
    version: str,
    origin: str,
    destination: str,
    date_from: str,
    date_to: str | None,
    adults: int,
    children: int,
    infants: int,
) -> dict | None:
    from curl_cffi import requests as cffi_requests

    proxies = _wizzair_module._get_curl_proxy()
    sess = cffi_requests.Session(
        impersonate=_wizzair_module._IMPERSONATE,
        proxies=proxies,
    )
    base = f"https://be.wizzair.com/{version}/Api"

    flight_list = [
        {
            "departureStation": origin,
            "arrivalStation": destination,
            "from": date_from,
            "to": date_from,
        }
    ]
    if date_to:
        flight_list.append(
            {
                "departureStation": destination,
                "arrivalStation": origin,
                "from": date_to,
                "to": date_to,
            }
        )

    body = {
        "flightList": flight_list,
        "adultCount": adults,
        "childCount": children,
        "infantCount": infants,
        "wdc": False,
        "priceType": "regular",
    }

    response = sess.post(
        f"{base}/search/timetableV2",
        json=body,
        headers=_wizzair_module._api_headers(),
        timeout=15,
    )
    if response.status_code == 200:
        return response.json()

    logger.warning(
        "Wizzair timetableV2: %d %s",
        response.status_code,
        response.text[:200],
    )
    return None


class WizzairConnectorClient(_BaseWizzairConnectorClient):
    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        # timetableV2 already returns both directions when return_from is set,
        # so _search_ow builds proper round-trip offers directly.
        return await self._search_ow(req)

    async def _search_single(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        loop = asyncio.get_running_loop()

        for attempt in range(1, _wizzair_module._MAX_ATTEMPTS + 1):
            try:
                version = await loop.run_in_executor(None, _wizzair_module._get_version_sync)
                logger.info(
                    "Wizzair: v%s, searching %s→%s on %s",
                    version,
                    req.origin,
                    req.destination,
                    req.date_from,
                )

                date_from = req.date_from.strftime("%Y-%m-%d")
                date_to = req.return_from.strftime("%Y-%m-%d") if req.return_from else None

                data = await loop.run_in_executor(
                    None,
                    functools.partial(
                        _search_timetable_sync_public,
                        version,
                        req.origin,
                        req.destination,
                        date_from,
                        date_to,
                        req.adults,
                        req.children,
                        req.infants,
                    ),
                )
                if data is not None:
                    cabin = {
                        "M": "economy",
                        "W": "premium_economy",
                        "C": "business",
                        "F": "first",
                    }.get(req.cabin_class or "M", "economy")
                    outbound = self._parse_timetable(
                        data.get("outboundFlights") or [],
                        req.date_from,
                        cabin,
                    )
                    inbound = self._parse_timetable(
                        data.get("returnFlights") or [],
                        req.return_from if req.return_from else req.date_from,
                        cabin,
                    )
                    offers = self._build_offers(req, outbound, inbound)
                    elapsed = time.monotonic() - t0
                    logger.info(
                        "Wizzair %s→%s returned %d offers in %.1fs",
                        req.origin,
                        req.destination,
                        len(offers),
                        elapsed,
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
                logger.warning(
                    "Wizzair: attempt %d/%d empty",
                    attempt,
                    _wizzair_module._MAX_ATTEMPTS,
                )
            except Exception as exc:
                logger.warning(
                    "Wizzair: attempt %d/%d error: %s",
                    attempt,
                    _wizzair_module._MAX_ATTEMPTS,
                    exc,
                )

        return self._empty(req)