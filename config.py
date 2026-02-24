from __future__ import annotations

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()

# Keys that get persisted to the database
_PERSIST_KEYS = (
    "stream_url", "whisper_model", "analysis_model", "alert_sensitivity",
    "custom_instructions", "chunk_duration_s", "openrouter_api_key",
    "google_maps_api_key", "map_default_lat", "map_default_lng",
    "event_timeout_minutes", "geocode_region", "geocode_max_radius_km",
)


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
    audio_dir: str = field(default_factory=lambda: os.getenv("AUDIO_DIR", "audio"))
    custom_instructions: str = field(default_factory=lambda: os.getenv("CUSTOM_INSTRUCTIONS", ""))
    google_maps_api_key: str = field(default_factory=lambda: os.getenv("GOOGLE_MAPS_API_KEY", ""))
    map_default_lat: float = field(default_factory=lambda: float(os.getenv("MAP_DEFAULT_LAT", "33.4418")))
    map_default_lng: float = field(default_factory=lambda: float(os.getenv("MAP_DEFAULT_LNG", "-94.0477")))
    event_timeout_minutes: int = field(default_factory=lambda: int(os.getenv("EVENT_TIMEOUT_MINUTES", "45")))
    geocode_region: str = field(default_factory=lambda: os.getenv("GEOCODE_REGION", "Texarkana, TX"))
    geocode_max_radius_km: float = field(default_factory=lambda: float(os.getenv("GEOCODE_MAX_RADIUS_KM", "50")))

    def to_dict(self) -> dict:
        return {
            "stream_url": self.stream_url,
            "whisper_model": self.whisper_model,
            "analysis_model": self.analysis_model,
            "alert_sensitivity": self.alert_sensitivity,
            "chunk_duration_s": self.chunk_duration_s,
            "custom_instructions": self.custom_instructions,
            "map_default_lat": self.map_default_lat,
            "map_default_lng": self.map_default_lng,
            "event_timeout_minutes": self.event_timeout_minutes,
            "geocode_region": self.geocode_region,
            "geocode_max_radius_km": self.geocode_max_radius_km,
        }

    def update(self, data: dict) -> None:
        for key in ("stream_url", "whisper_model", "analysis_model", "alert_sensitivity", "custom_instructions"):
            if key in data:
                setattr(self, key, data[key])
        if "openrouter_api_key" in data and data["openrouter_api_key"]:
            self.openrouter_api_key = data["openrouter_api_key"]
        if "google_maps_api_key" in data and data["google_maps_api_key"]:
            self.google_maps_api_key = data["google_maps_api_key"]
        if "chunk_duration_s" in data:
            self.chunk_duration_s = int(data["chunk_duration_s"])
        if "event_timeout_minutes" in data:
            self.event_timeout_minutes = int(data["event_timeout_minutes"])
        if "map_default_lat" in data:
            self.map_default_lat = float(data["map_default_lat"])
        if "map_default_lng" in data:
            self.map_default_lng = float(data["map_default_lng"])
        if "geocode_region" in data:
            self.geocode_region = data["geocode_region"]
        if "geocode_max_radius_km" in data:
            self.geocode_max_radius_km = float(data["geocode_max_radius_km"])
        # Persist to database
        self._save()

    def load_saved(self) -> None:
        """Load settings saved in the database, overriding env-var defaults."""
        from database import load_settings
        saved = load_settings(self.db_path)
        if not saved:
            return
        _str_keys = ("stream_url", "whisper_model", "analysis_model",
                      "alert_sensitivity", "custom_instructions",
                      "openrouter_api_key", "google_maps_api_key",
                      "geocode_region")
        for key in _str_keys:
            if key in saved and saved[key]:
                setattr(self, key, saved[key])
        if "chunk_duration_s" in saved:
            self.chunk_duration_s = int(saved["chunk_duration_s"])
        if "map_default_lat" in saved:
            self.map_default_lat = float(saved["map_default_lat"])
        if "map_default_lng" in saved:
            self.map_default_lng = float(saved["map_default_lng"])
        if "event_timeout_minutes" in saved:
            self.event_timeout_minutes = int(saved["event_timeout_minutes"])
        if "geocode_max_radius_km" in saved:
            self.geocode_max_radius_km = float(saved["geocode_max_radius_km"])

    def _save(self) -> None:
        """Persist current settings to the database."""
        from database import save_settings
        data = {}
        for key in _PERSIST_KEYS:
            data[key] = getattr(self, key)
        save_settings(self.db_path, data)


config = Config()
