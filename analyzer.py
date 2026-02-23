from __future__ import annotations

import json
import logging

import httpx
from openai import AsyncOpenAI

logger = logging.getLogger(__name__)

SENSITIVITY_THRESHOLDS = {
    "low": "Only alert on clearly critical events: active shootings, major explosions, officer down, mass casualty incidents.",
    "medium": "Alert on significant events: shootings, armed suspects, pursuits, structure fires, serious accidents, missing persons.",
    "high": "Alert on most notable events: any weapons calls, pursuits, fires, accidents with injuries, domestic disturbances, drug activity.",
}

SYSTEM_PROMPT = """You are a police radio transcript analyst. Analyze the following transcript from a police/emergency radio scanner and determine if it contains any breaking news or significant events worth alerting on.

Sensitivity level: {sensitivity_instruction}

Respond with ONLY a JSON object (no markdown, no code fences):
{{
  "has_alert": true/false,
  "summary": "Brief description of the event",
  "severity": "critical|high|medium|low",
  "category": "shooting|pursuit|fire|accident|medical|missing_person|robbery|assault|drug_activity|hazmat|other",
  "event_id": null,
  "event_title": "Short title for the event",
  "event_status": "active",
  "location": "Street address, intersection, or landmark mentioned"
}}

event_id: Set to the ID of an existing active event if this transcript is an update to that event. Set to null if this is a new event.
event_title: A short descriptive title for new events (e.g. "Vehicle pursuit on Highway 71"). Ignored when event_id is set to an existing event.
event_status: "active" if the event is ongoing, "resolved" if the event has concluded (suspect in custody, fire extinguished, scene cleared, etc.)
location: The street address, intersection, or specific location mentioned in the radio traffic (e.g. "400 block of Main Street", "Highway 71 and State Line Ave"). Set to "" if no location mentioned.

If no significant event is detected, respond:
{{"has_alert": false, "summary": "", "severity": "low", "category": "other", "event_id": null, "event_title": "", "event_status": "active", "location": ""}}

Focus on NEW events or MEANINGFUL UPDATES to existing events. Do NOT re-alert unless there is a meaningful update (status change, escalation, new details).
Routine traffic stops, status checks, and administrative radio chatter are NOT alerts.

{active_events_section}
{recent_alerts_section}"""


def get_base_prompt() -> str:
    """Return the base system prompt template (for display in settings)."""
    return SYSTEM_PROMPT


async def analyze_transcript(
    transcript: str,
    api_key: str,
    model: str = "google/gemini-2.0-flash-001",
    sensitivity: str = "medium",
    recent_alerts: list[str] | None = None,
    custom_instructions: str = "",
    active_events: list[dict] | None = None,
) -> dict | None:
    """Analyze transcript text for breaking news. Returns alert dict or None."""
    if not transcript.strip() or not api_key:
        return None

    sensitivity_instruction = SENSITIVITY_THRESHOLDS.get(sensitivity, SENSITIVITY_THRESHOLDS["medium"])

    if recent_alerts:
        bullets = "\n".join(f"- {a}" for a in recent_alerts)
        recent_alerts_section = f"The following events have ALREADY been alerted on. Do NOT alert on these again:\n{bullets}"
    else:
        recent_alerts_section = ""

    if active_events:
        event_lines = []
        for ev in active_events:
            event_lines.append(f"- ID {ev['id']}: [{ev['category']}] {ev['title']} (severity: {ev['severity']}, since {ev['created_at']})")
        active_events_section = "ACTIVE EVENTS (set event_id to one of these if the transcript updates it):\n" + "\n".join(event_lines)
    else:
        active_events_section = ""

    system_content = SYSTEM_PROMPT.format(
        sensitivity_instruction=sensitivity_instruction,
        recent_alerts_section=recent_alerts_section,
        active_events_section=active_events_section,
    )

    if custom_instructions.strip():
        system_content += "\n\nAdditional instructions from the operator:\n" + custom_instructions.strip()

    client = AsyncOpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )

    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_content},
                {"role": "user", "content": f"Analyze this police radio transcript:\n\n{transcript}"},
            ],
            temperature=0.1,
            max_tokens=300,
        )

        content = response.choices[0].message.content.strip()
        # Strip markdown code fences if present
        if content.startswith("```"):
            content = content.split("\n", 1)[1] if "\n" in content else content[3:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()

        # Extract JSON object even if surrounded by extra text
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1:
            content = content[start:end + 1]

        result = json.loads(content)
        logger.info("LLM result: has_alert=%s severity=%s category=%s location=%s",
                     result.get("has_alert"), result.get("severity"), result.get("category"), result.get("location", ""))

        if result.get("has_alert"):
            return {
                "summary": result.get("summary", ""),
                "severity": result.get("severity", "medium"),
                "category": result.get("category", "other"),
                "event_id": result.get("event_id"),
                "event_title": result.get("event_title", ""),
                "event_status": result.get("event_status", "active"),
                "location": result.get("location", ""),
            }
        return None

    except json.JSONDecodeError:
        logger.warning("Failed to parse analyzer response: %s", content)
        return None
    except Exception:
        logger.exception("Error analyzing transcript")
        return None


async def fetch_models(api_key: str) -> list[dict]:
    """Fetch available models from OpenRouter."""
    if not api_key:
        return []
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://openrouter.ai/api/v1/models",
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            models = []
            for m in data.get("data", []):
                models.append({
                    "id": m["id"],
                    "name": m.get("name", m["id"]),
                })
            models.sort(key=lambda x: x["name"])
            return models
    except Exception:
        logger.exception("Error fetching OpenRouter models")
        return []
