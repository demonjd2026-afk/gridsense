"""Smoke tests for the ENTSO-E producer."""

from __future__ import annotations

import json

from src.main import (
    COUNTRIES,
    PSR_TYPE_NAMES,
    extract_generation_mix,
    make_period_window,
    shape_payload,
)

# Minimal A75 response shape - one TimeSeries with one Point.
SAMPLE_SINGLE_SERIES = {
    "GL_MarketDocument": {
        "TimeSeries": {
            "MktPSRType": {"psrType": "B14"},  # Nuclear
            "Period": {
                "timeInterval": {"start": "2026-05-13T02:00Z", "end": "2026-05-13T03:00Z"},
                "resolution": "PT60M",
                "Point": {"position": "1", "quantity": "44000.0"},
            },
        },
    }
}

# Response with multiple TimeSeries (typical for a real country).
SAMPLE_MULTI_SERIES = {
    "GL_MarketDocument": {
        "TimeSeries": [
            {
                "MktPSRType": {"psrType": "B14"},  # Nuclear
                "Period": {
                    "resolution": "PT60M",
                    "Point": [{"position": "1", "quantity": "44000"}],
                },
            },
            {
                "MktPSRType": {"psrType": "B19"},  # Wind onshore
                "Period": {
                    "resolution": "PT60M",
                    "Point": [{"position": "1", "quantity": "8500"}],
                },
            },
            {
                "MktPSRType": {"psrType": "B16"},  # Solar
                "Period": {
                    "resolution": "PT60M",
                    "Point": [{"position": "1", "quantity": "0"}],
                },
            },
        ],
    }
}


def test_extract_handles_single_timeseries_dict() -> None:
    mix = extract_generation_mix(SAMPLE_SINGLE_SERIES)
    assert mix == {"B14": 44000.0}


def test_extract_handles_multiple_timeseries_list() -> None:
    mix = extract_generation_mix(SAMPLE_MULTI_SERIES)
    assert mix == {"B14": 44000.0, "B19": 8500.0, "B16": 0.0}


def test_extract_empty_response() -> None:
    mix = extract_generation_mix({"GL_MarketDocument": {}})
    assert mix == {}


def test_shape_payload_structure() -> None:
    mix = {"B14": 44000.0, "B19": 8500.0, "B16": 0.0}
    payload = shape_payload(
        "FR",
        "France",
        "10YFR-RTE------C",
        "2026-05-13T02:00:00+00:00",
        "2026-05-13T03:00:00+00:00",
        mix,
    )
    assert payload["country_code"] == "FR"
    assert payload["total_generation_mw"] == 52500.0
    assert len(payload["generation_mix"]) == 3
    # generation_mix is sorted by psr_type
    psr_types = [item["psr_type"] for item in payload["generation_mix"]]
    assert psr_types == sorted(psr_types)
    # Each entry has the readable name
    nuclear = next(m for m in payload["generation_mix"] if m["psr_type"] == "B14")
    assert nuclear["name"] == "Nuclear"


def test_shape_payload_serializable() -> None:
    payload = shape_payload(
        "DE",
        "Germany",
        "10Y1001A1001A83F",
        "2026-05-13T02:00:00+00:00",
        "2026-05-13T03:00:00+00:00",
        {"B16": 5000.5},
    )
    json.dumps(payload)


def test_make_period_window_format() -> None:
    start, end, anchor = make_period_window()
    assert len(start) == 12
    assert len(end) == 12
    assert start.isdigit()
    assert end.isdigit()
    # anchor is exactly on the hour
    assert anchor.minute == 0
    assert anchor.second == 0


def test_countries_constant() -> None:
    assert len(COUNTRIES) == 6
    # All EIC codes are 16 chars
    for _, eic, _ in COUNTRIES:
        assert len(eic) == 16


def test_psr_type_names_complete() -> None:
    # 20 generation B-codes + 5 infrastructure/storage B-codes + 3 A-codes
    assert len(PSR_TYPE_NAMES) == 28
    assert "B14" in PSR_TYPE_NAMES  # Nuclear
    assert "B16" in PSR_TYPE_NAMES  # Solar
    assert "B19" in PSR_TYPE_NAMES  # Wind onshore
    assert "B25" in PSR_TYPE_NAMES  # Energy storage (seen in FR data)
