"""
Sky Airline connector — real-time search via farequoting API.

Sky Airline (IATA: H2) is Chile's largest low-cost carrier.
Operates 45+ domestic and regional routes from SCL hub.
Destinations in Chile, Peru, Argentina, Brazil, Uruguay.

Strategy (httpx, no browser):
  1. POST api.skyairline.com/farequoting/v1/search/flight?stage=IS
  2. Parse branded fare results from itineraryParts
  3. Each flight has segments with times + brandOffers with prices
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Optional

import httpx

from models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_SEARCH_URL = "https://api.skyairline.com/farequoting/v1/search/flight?stage=IS"
_SUBSCRIPTION_KEY = "4c998b33d2aa4e8aba0f9a63d4c04d7d"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
    "Ocp-Apim-Subscription-Key": _SUBSCRIPTION_KEY,
    "channel": "WEB",
    "homemarket": "CL",
}


class SkyAirlineConnectorClient:
    """Sky Airline Chile — real-time farequoting search API (httpx)."""

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                timeout=self.timeout, headers=_HEADERS, follow_redirects=True
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        t0 = time.monotonic()
        client = await self._client()

        payload = {
            "cabinClass": "Economy",
            "currency": None,
            "awardBooking": False,
            "pointOfSale": "CL",
            "searchType": "BRANDED",
            "itineraryParts": [{
                "origin": {"code": req.origin, "useNearbyLocations": False},
                "destination": {"code": req.destination, "useNearbyLocations": False},
                "departureDate": {"date": req.date_from.strftime("%Y-%m-%d")},
                "selectedOfferRef": None,
                "plusMinusDays": None,
            }],
            "passengers": {
                "ADT": req.adults or 1,
                "CHD": req.children or 0,
                "INF": req.infants or 0,
            },
        }

        try:
            resp = await client.post(_SEARCH_URL, json=payload)
            if resp.status_code != 200:
                logger.warning("Sky Airline API: %d %s", resp.status_code, resp.text[:200])
                return self._empty(req)
            data = resp.json()
        except Exception as e:
            logger.error("Sky Airline API error: %s", e)
            return self._empty(req)

        offers = self._parse(data, req)
        offers.sort(key=lambda o: o.price if o.price > 0 else float("inf"))

        elapsed = time.monotonic() - t0
        logger.info("Sky Airline %s→%s: %d offers in %.1fs", req.origin, req.destination, len(offers), elapsed)

        h = hashlib.md5(f"skyairline{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency=offers[0].currency if offers else "CLP",
            offers=offers,
            total_results=len(offers),
        )

    @staticmethod
    def _parse(data: dict, req: FlightSearchRequest) -> list[FlightOffer]:
        offers: list[FlightOffer] = []
        parts = data.get("itineraryParts", [])
        if not parts:
            return offers

        # Part 0 = outbound flights
        outbound = parts[0] if isinstance(parts[0], list) else []
        target_date = req.date_from.strftime("%Y-%m-%d")

        for flight in outbound:
            if not isinstance(flight, dict):
                continue

            segments_data = flight.get("segments", [])
            brand_offers = flight.get("fares", [])
            stops = flight.get("stops", 0)
            total_dur = flight.get("totalDuration", 0)

            # Build segments
            segments: list[FlightSegment] = []
            for seg in segments_data:
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

                segments.append(FlightSegment(
                    airline=fl.get("airlineCode", "H2"),
                    airline_name="Sky Airline",
                    flight_no=f"H2{fl.get('flightNumber', '')}",
                    origin=seg.get("origin", {}).get("code", "") if isinstance(seg.get("origin"), dict) else seg.get("origin", ""),
                    destination=seg.get("destination", {}).get("code", "") if isinstance(seg.get("destination"), dict) else seg.get("destination", ""),
                    departure=dep_dt,
                    arrival=arr_dt,
                    duration_seconds=seg.get("duration", 0) * 60,
                    cabin_class=seg.get("cabinClass", "Economy"),
                ))

            if not segments:
                continue

            route = FlightRoute(
                segments=segments,
                total_duration_seconds=total_dur * 60,
                stopovers=stops,
            )

            # Each fare brand (ZO, LT, ED, MF, PL) is a separate offer
            for fare in brand_offers:
                pax_prices = fare.get("priceByPassengerTypes", [])
                if not pax_prices:
                    continue
                fare_info = pax_prices[0].get("fare", {})
                amount = fare_info.get("amount")
                currency = fare_info.get("currency", "CLP")
                if not amount or float(amount) <= 0:
                    continue

                price_f = round(float(amount), 2)
                brand_id = fare.get("brandId", "")

                fid = hashlib.md5(
                    f"h2_{req.origin}{req.destination}{dep_str}{brand_id}{price_f}".encode()
                ).hexdigest()[:12]

                offers.append(FlightOffer(
                    id=f"h2_{fid}",
                    price=price_f,
                    currency=currency,
                    price_formatted=f"{price_f:,.0f} {currency}",
                    outbound=route,
                    inbound=None,
                    airlines=["Sky Airline"],
                    owner_airline="H2",
                    conditions={"fare_brand": brand_id},
                    booking_url=(
                        f"https://initial-sale.skyairline.com/es/chile"
                        f"?origin={req.origin}&destination={req.destination}"
                        f"&departureDate={target_date}"
                        f"&ADT={req.adults or 1}&CHD={req.children or 0}&INF={req.infants or 0}"
                        f"&flightType=OW"
                    ),
                    is_locked=False,
                    source="skyairline_direct",
                    source_tier="free",
                ))

        return offers

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        h = hashlib.md5(f"skyairline{req.origin}{req.destination}{req.date_from}".encode()).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{h}",
            origin=req.origin,
            destination=req.destination,
            currency="CLP",
            offers=[],
            total_results=0,
        )
