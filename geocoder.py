from __future__ import annotations

import logging
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


async def geocode_location(
    location_text: str,
    api_key: str,
    db_path: str,
    bias_lat: float,
    bias_lng: float,
) -> tuple[float, float] | None:
    """Geocode a location string via Google Geocoding API with cache-first pattern.

    Returns (lat, lng) tuple or None on failure.
    """
    if not location_text or not location_text.strip() or not api_key:
        return None

    address_key = _normalize_address(location_text)
    if not address_key:
        return None

    # Check cache first
    cached = await get_cached_geocode(db_path, address_key)
    if cached:
        return cached

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
                    "address": location_text,
                    "bounds": bounds,
                    "key": api_key,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        if data.get("status") != "OK" or not data.get("results"):
            logger.debug("Geocoding returned no results for: %s (status: %s)", location_text, data.get("status"))
            return None

        result = data["results"][0]
        loc = result["geometry"]["location"]
        lat = loc["lat"]
        lng = loc["lng"]
        formatted = result.get("formatted_address", "")

        # Cache the result
        await insert_geocode_cache(db_path, address_key, lat, lng, formatted)

        return (lat, lng)

    except Exception:
        logger.exception("Geocoding failed for: %s", location_text)
        return None
