"""Police 10-code and signal-code mappings for contradiction detection."""
from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# Common police 10-codes (varies by jurisdiction but these are widely used)
TEN_CODES = {
    "10-0": {"meaning": "caution", "category": None, "severity": None},
    "10-1": {"meaning": "unable to copy", "category": None, "severity": None},
    "10-4": {"meaning": "acknowledgement", "category": None, "severity": None},
    "10-6": {"meaning": "busy", "category": None, "severity": None},
    "10-7": {"meaning": "out of service", "category": None, "severity": None},
    "10-8": {"meaning": "in service", "category": None, "severity": None},
    "10-9": {"meaning": "repeat", "category": None, "severity": None},
    "10-10": {"meaning": "fight in progress", "category": "assault", "severity": "medium"},
    "10-15": {"meaning": "prisoner in custody", "category": None, "severity": None},
    "10-16": {"meaning": "domestic disturbance", "category": "assault", "severity": "medium"},
    "10-17": {"meaning": "meet complainant", "category": None, "severity": None},
    "10-20": {"meaning": "location", "category": None, "severity": None},
    "10-22": {"meaning": "disregard", "category": None, "severity": None},
    "10-23": {"meaning": "arrived at scene", "category": None, "severity": None},
    "10-27": {"meaning": "license check", "category": None, "severity": None},
    "10-28": {"meaning": "registration check", "category": None, "severity": None},
    "10-29": {"meaning": "check for wanted", "category": None, "severity": None},
    "10-32": {"meaning": "person with gun", "category": "shooting", "severity": "high"},
    "10-33": {"meaning": "emergency", "category": None, "severity": "high"},
    "10-34": {"meaning": "riot", "category": "assault", "severity": "critical"},
    "10-35": {"meaning": "major crime alert", "category": None, "severity": "high"},
    "10-39": {"meaning": "urgent - use lights and siren", "category": None, "severity": "high"},
    "10-40": {"meaning": "silent run", "category": None, "severity": None},
    "10-45": {"meaning": "dead animal", "category": None, "severity": None},
    "10-46": {"meaning": "assist motorist", "category": None, "severity": None},
    "10-50": {"meaning": "accident", "category": "accident", "severity": "medium"},
    "10-51": {"meaning": "wrecker needed", "category": "accident", "severity": "low"},
    "10-52": {"meaning": "ambulance needed", "category": "medical", "severity": "medium"},
    "10-53": {"meaning": "road blocked", "category": "accident", "severity": "low"},
    "10-54": {"meaning": "livestock on highway", "category": "other", "severity": "low"},
    "10-55": {"meaning": "intoxicated driver", "category": "accident", "severity": "medium"},
    "10-56": {"meaning": "intoxicated pedestrian", "category": "other", "severity": "low"},
    "10-57": {"meaning": "hit and run", "category": "accident", "severity": "medium"},
    "10-60": {"meaning": "squad in vicinity", "category": None, "severity": None},
    "10-61": {"meaning": "personnel in area", "category": None, "severity": None},
    "10-62": {"meaning": "reply to message", "category": None, "severity": None},
    "10-65": {"meaning": "net message assignment", "category": None, "severity": None},
    "10-66": {"meaning": "net message cancellation", "category": None, "severity": None},
    "10-67": {"meaning": "clear for net message", "category": None, "severity": None},
    "10-70": {"meaning": "fire alarm", "category": "fire", "severity": "medium"},
    "10-71": {"meaning": "advise nature of fire", "category": "fire", "severity": "medium"},
    "10-72": {"meaning": "report progress on fire", "category": "fire", "severity": "medium"},
    "10-73": {"meaning": "smoke report", "category": "fire", "severity": "low"},
    "10-78": {"meaning": "need assistance", "category": None, "severity": "high"},
    "10-79": {"meaning": "notify coroner", "category": "medical", "severity": "critical"},
    "10-80": {"meaning": "pursuit in progress", "category": "pursuit", "severity": "high"},
    "10-85": {"meaning": "delayed", "category": None, "severity": None},
    "10-86": {"meaning": "officer on duty", "category": None, "severity": None},
    "10-91": {"meaning": "pick up prisoner", "category": None, "severity": None},
    "10-97": {"meaning": "check signal", "category": None, "severity": None},
    "10-98": {"meaning": "prison break", "category": "other", "severity": "critical"},
    "10-99": {"meaning": "officer needs help", "category": None, "severity": "critical"},
}

# Pattern to detect 10-codes in text
_TEN_CODE_RE = re.compile(r'\b10-(\d{1,2})\b', re.IGNORECASE)

# Also detect spelled-out versions
_SPELLED_TEN_CODE_RE = re.compile(
    r'\bten[-\s](\d{1,2})\b', re.IGNORECASE
)


def extract_codes(transcript: str) -> list[dict]:
    """Extract 10-codes found in transcript text.

    Returns list of {code, meaning, category, severity}.
    """
    found = []
    seen = set()

    for pattern in (_TEN_CODE_RE, _SPELLED_TEN_CODE_RE):
        for match in pattern.finditer(transcript):
            num = match.group(1)
            code = f"10-{num}"
            if code in seen:
                continue
            seen.add(code)
            info = TEN_CODES.get(code)
            if info:
                found.append({
                    "code": code,
                    "meaning": info["meaning"],
                    "category": info["category"],
                    "severity": info["severity"],
                })

    return found


def detect_analysis_contradictions(transcript: str, llm_result: dict) -> list[dict]:
    """Detect contradictions between 10-codes in transcript and LLM analysis.

    Returns list of contradiction descriptions.
    """
    codes = extract_codes(transcript)
    if not codes:
        return []

    llm_category = llm_result.get("category", "")
    llm_severity = llm_result.get("severity", "")
    contradictions = []

    severity_order = {"low": 0, "medium": 1, "high": 2, "critical": 3}

    for code_info in codes:
        code_cat = code_info["category"]
        code_sev = code_info["severity"]

        if not code_cat:
            continue

        # Category mismatch
        if code_cat != llm_category and llm_category != "other":
            contradictions.append({
                "type": "category_mismatch",
                "code": code_info["code"],
                "code_meaning": code_info["meaning"],
                "code_category": code_cat,
                "llm_category": llm_category,
                "message": f"{code_info['code']} ({code_info['meaning']}) suggests '{code_cat}' but LLM classified as '{llm_category}'",
            })

        # Severity mismatch (code suggests higher severity)
        if code_sev and llm_severity:
            if severity_order.get(code_sev, 0) > severity_order.get(llm_severity, 0) + 1:
                contradictions.append({
                    "type": "severity_mismatch",
                    "code": code_info["code"],
                    "code_meaning": code_info["meaning"],
                    "code_severity": code_sev,
                    "llm_severity": llm_severity,
                    "message": f"{code_info['code']} ({code_info['meaning']}) suggests severity '{code_sev}' but LLM assigned '{llm_severity}'",
                })

    if contradictions:
        logger.warning("Contradictions detected: %s", [c["message"] for c in contradictions])

    return contradictions
