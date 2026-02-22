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
  "category": "shooting|pursuit|fire|accident|medical|missing_person|robbery|assault|drug_activity|hazmat|other"
}}

If no significant event is detected, respond:
{{"has_alert": false, "summary": "", "severity": "low", "category": "other"}}

Focus on NEW events. Routine traffic stops, status checks, and administrative radio chatter are NOT alerts."""


async def analyze_transcript(
    transcript: str,
    api_key: str,
    model: str = "google/gemini-2.0-flash-001",
    sensitivity: str = "medium",
) -> dict | None:
    """Analyze transcript text for breaking news. Returns alert dict or None."""
    if not transcript.strip() or not api_key:
        return None

    sensitivity_instruction = SENSITIVITY_THRESHOLDS.get(sensitivity, SENSITIVITY_THRESHOLDS["medium"])

    client = AsyncOpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )

    try:
        response = await client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT.format(sensitivity_instruction=sensitivity_instruction)},
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

        if result.get("has_alert"):
            return {
                "summary": result.get("summary", ""),
                "severity": result.get("severity", "medium"),
                "category": result.get("category", "other"),
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
