from __future__ import annotations

import logging
import math
import re

import httpx

from database import get_cached_geocode, insert_geocode_cache

logger = logging.getLogger(__name__)


def _normalize_address(text: str) -> str:
    """Normalize address text for cache key: lowercase, collapse whitespace, strip punctuation."""
    text = text.strip().lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Compute great-circle distance in km between two points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


async def geocode_location(
    location_text: str,
    api_key: str,
    db_path: str,
    bias_lat: float,
    bias_lng: float,
    region_hint: str = "",
    max_radius_km: float = 50.0,
) -> tuple[float, float] | None:
    """Geocode a location string via Google Geocoding API with cache-first pattern.

    Returns (lat, lng) tuple or None on failure.
    """
    if not location_text or not location_text.strip() or not api_key:
        return None

    # Cache key uses original text (not the suffixed version)
    address_key = _normalize_address(location_text)
    if not address_key:
        return None

    # Check cache first
    cached = await get_cached_geocode(db_path, address_key)
    if cached:
        return cached

    # Append region hint to improve geocoding accuracy
    query_address = location_text
    if region_hint:
        query_address = f"{location_text}, {region_hint}"

    # Build bounds ~30km around configured center for bias
    offset = 0.27  # ~30km in degrees
    bounds = (
        f"{bias_lat - offset},{bias_lng - offset}"
        f"|{bias_lat + offset},{bias_lng + offset}"
    )

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://maps.googleapis.com/maps/api/geocode/json",
                params={
                    "address": query_address,
                    "bounds": bounds,
                    "components": "country:US",
                    "key": api_key,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        if data.get("status") != "OK" or not data.get("results"):
            logger.debug("Geocoding returned no results for: %s (status: %s)", query_address, data.get("status"))
            return None

        result = data["results"][0]
        loc = result["geometry"]["location"]
        lat = loc["lat"]
        lng = loc["lng"]
        formatted = result.get("formatted_address", "")

        # Distance filter: reject results too far from center
        if max_radius_km > 0:
            dist = _haversine_km(bias_lat, bias_lng, lat, lng)
            if dist > max_radius_km:
                logger.warning(
                    "Geocode result for %r is %.1f km from center (max %.1f km), discarding: %s",
                    location_text, dist, max_radius_km, formatted,
                )
                return None

        # Cache the result
        await insert_geocode_cache(db_path, address_key, lat, lng, formatted)

        return (lat, lng)

    except Exception:
        logger.exception("Geocoding failed for: %s", query_address)
        return None
