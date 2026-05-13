"""Smoke tests for the open-meteo producer."""

from __future__ import annotations

import json

import pytest

from src.main import CITIES, first_hour_snapshot

SAMPLE_RESPONSE = {
    "latitude": 51.5,
    "longitude": -0.12,
    "elevation": 35.0,
    "hourly_units": {
        "temperature_2m": "°C",
        "wind_speed_10m": "km/h",
        "cloud_cover": "%",
        "shortwave_radiation": "W/m²",
    },
    "hourly": {
        "time": ["2026-05-13T08:00", "2026-05-13T09:00"],
        "temperature_2m": [12.4, 13.1],
        "wind_speed_10m": [18.0, 19.4],
        "cloud_cover": [60, 55],
        "shortwave_radiation": [220.0, 280.0],
    },
}


def test_first_hour_snapshot_shape() -> None:
    snap = first_hour_snapshot(SAMPLE_RESPONSE, "London")
    assert snap["city"] == "London"
    assert snap["time"] == "2026-05-13T08:00"
    assert snap["temperature_2m"] == 12.4
    assert snap["units"]["wind_speed_10m"] == "km/h"


def test_first_hour_snapshot_serializable() -> None:
    """Envelope payloads must be JSON-serializable for Kafka send."""
    snap = first_hour_snapshot(SAMPLE_RESPONSE, "London")
    json.dumps(snap)  # raises if not serializable


def test_first_hour_snapshot_empty_raises() -> None:
    empty = {**SAMPLE_RESPONSE, "hourly": {"time": []}}
    with pytest.raises(ValueError, match="empty hourly forecast"):
        first_hour_snapshot(empty, "London")


def test_cities_constant() -> None:
    assert len(CITIES) == 6
    assert all(len(c) == 3 for c in CITIES)
    assert all(isinstance(c[0], float) for c in CITIES)
