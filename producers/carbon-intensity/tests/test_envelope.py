"""Unit tests for the carbon-intensity producer.

These tests cover the envelope shape, checksum determinism, and basic
contract assumptions. They don't exercise the Kafka or HTTP layers
(those are integration tests that need real Event Hubs / mocks).
"""

from __future__ import annotations

import hashlib
import json

import pytest

from src.main import SOURCE_NAME, SOURCE_VERSION, build_envelope

SAMPLE_PAYLOAD = {
    "from": "2026-05-12T15:30Z",
    "to": "2026-05-12T16:00Z",
    "intensity": {"forecast": 180, "index": "moderate"},
    "generationmix": [
        {"fuel": "gas", "perc": 30.1},
        {"fuel": "wind", "perc": 45.2},
    ],
}


def test_envelope_has_required_fields():
    """Envelope must contain every field downstream consumers depend on."""
    env = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    required = {
        "event_id",
        "source",
        "source_version",
        "ingested_at",
        "event_time",
        "region",
        "payload",
        "checksum",
    }
    assert required.issubset(env.keys())


def test_envelope_source_metadata():
    env = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    assert env["source"] == SOURCE_NAME
    assert env["source_version"] == SOURCE_VERSION


def test_envelope_preserves_raw_payload():
    """Downstream replay needs the upstream payload unchanged."""
    env = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    assert env["payload"] == SAMPLE_PAYLOAD


def test_envelope_region_is_routing_key():
    """Region must be exposed as a top-level field for partitioning."""
    env = build_envelope(SAMPLE_PAYLOAD, "GB-NTH")
    assert env["region"] == "GB-NTH"


def test_envelope_event_time_from_payload():
    env = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    assert env["event_time"] == SAMPLE_PAYLOAD["from"]


def test_event_id_is_unique_per_envelope():
    a = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    b = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    assert a["event_id"] != b["event_id"]


def test_checksum_format():
    """Checksum must be `sha256:<hex>` for downstream verifiers."""
    env = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    assert env["checksum"].startswith("sha256:")
    hex_part = env["checksum"].split(":", 1)[1]
    assert len(hex_part) == 64
    assert all(c in "0123456789abcdef" for c in hex_part)


def test_checksum_deterministic_for_same_payload():
    """Same payload + same region must produce the same checksum (modulo
    event_id and ingested_at, which we recompute separately)."""
    a = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    b = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    assert a["checksum"] == b["checksum"]


def test_checksum_matches_canonical_payload():
    """Checksum must be over the sorted-keys canonical JSON of the payload."""
    env = build_envelope(SAMPLE_PAYLOAD, "GB-LON")
    expected = hashlib.sha256(
        json.dumps(SAMPLE_PAYLOAD, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    assert env["checksum"] == f"sha256:{expected}"


def test_envelope_handles_missing_event_time():
    """If upstream doesn't provide `from`, event_time is None, not a crash."""
    payload_no_from = {"intensity": {"forecast": 100}}
    env = build_envelope(payload_no_from, "GB-LON")
    assert env["event_time"] is None


@pytest.mark.parametrize(
    "region",
    ["GB-LON", "GB-NTH", "GB-SCT", "North Scotland", "South West England"],
)
def test_envelope_accepts_various_region_formats(region):
    env = build_envelope(SAMPLE_PAYLOAD, region)
    assert env["region"] == region
