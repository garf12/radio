from __future__ import annotations

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    stream_url: str = field(default_factory=lambda: os.getenv("STREAM_URL", ""))
    openrouter_api_key: str = field(default_factory=lambda: os.getenv("OPENROUTER_API_KEY", ""))
    whisper_model: str = field(default_factory=lambda: os.getenv("WHISPER_MODEL", "base"))
    analysis_model: str = field(default_factory=lambda: os.getenv("ANALYSIS_MODEL", "google/gemini-2.0-flash-001"))
    alert_sensitivity: str = field(default_factory=lambda: os.getenv("ALERT_SENSITIVITY", "medium"))
    host: str = field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: int(os.getenv("PORT", "8000")))
    chunk_duration_s: int = field(default_factory=lambda: int(os.getenv("CHUNK_DURATION_S", "30")))
    db_path: str = field(default_factory=lambda: os.getenv("DB_PATH", "scanner.db"))

    def to_dict(self) -> dict:
        return {
            "stream_url": self.stream_url,
            "whisper_model": self.whisper_model,
            "analysis_model": self.analysis_model,
            "alert_sensitivity": self.alert_sensitivity,
            "chunk_duration_s": self.chunk_duration_s,
        }

    def update(self, data: dict) -> None:
        for key in ("stream_url", "whisper_model", "analysis_model", "alert_sensitivity"):
            if key in data:
                setattr(self, key, data[key])
        if "openrouter_api_key" in data and data["openrouter_api_key"]:
            self.openrouter_api_key = data["openrouter_api_key"]
        if "chunk_duration_s" in data:
            self.chunk_duration_s = int(data["chunk_duration_s"])


config = Config()
