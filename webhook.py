from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


async def send_webhook_alert(url: str, alert: dict, event: dict | None = None) -> None:
    """Fire-and-forget POST to an n8n (or any) webhook URL.

    Never raises — errors are logged and silently swallowed so the
    pipeline is never interrupted by webhook issues.
    """
    payload = {
        "alert_id": alert.get("id"),
        "summary": alert.get("summary", ""),
        "severity": alert.get("severity", ""),
        "category": alert.get("category", ""),
        "timestamp": alert.get("timestamp", ""),
        "transcript": alert.get("transcript", ""),
    }
    if event:
        payload["event"] = {
            "id": event.get("id"),
            "title": event.get("title", ""),
            "location": event.get("location_text", ""),
            "latitude": event.get("latitude"),
            "longitude": event.get("longitude"),
            "status": event.get("status", ""),
        }

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
        logger.info("Webhook sent successfully to %s (status %d)", url, resp.status_code)
    except Exception:
        logger.exception("Failed to send webhook to %s", url)
