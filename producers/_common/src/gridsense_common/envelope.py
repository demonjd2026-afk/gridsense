"""Canonical event envelope for all GridSense producers.

Every event published to Event Hubs is wrapped in the same envelope shape
so downstream consumers (Bronze layer, ML pipelines, dashboards) can rely
on a stable schema regardless of which producer emitted the event.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import UTC, datetime
from typing import Any


def build_envelope(
    payload: dict[str, Any],
    region: str,
    source_name: str,
    source_version: str = "v1",
    event_time: str | None = None,
) -> dict[str, Any]:
    """Wrap a raw API payload in the canonical event envelope.

    Args:
        payload: The raw upstream API payload (will be nested under `payload`).
        region: Routing key for partitioning (e.g. "GB-LON", "London").
        source_name: Producer identifier (e.g. "uk-carbon-intensity", "open-meteo").
        source_version: Schema version of this producer's output. Bump when
            the shape of `payload` changes in a breaking way.
        event_time: ISO-8601 timestamp of the upstream measurement. If None,
            we fall back to payload["from"] (carbon-intensity convention) or
            payload["time"] (open-meteo convention).

    The envelope adds:
        event_id    : globally-unique UUID for dedup
        source      : which producer published this
        ingested_at : when we (the producer) generated this event
        event_time  : when the upstream measurement was actually for
        region      : routing key for partitioning
        checksum    : sha256 of the canonical payload for tamper detection
    """
    if event_time is None:
        event_time = payload.get("from") or payload.get("time")

    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return {
        "event_id": str(uuid.uuid4()),
        "source": source_name,
        "source_version": source_version,
        "ingested_at": datetime.now(UTC).isoformat(),
        "event_time": event_time,
        "region": region,
        "payload": payload,
        "checksum": f"sha256:{hashlib.sha256(body.encode()).hexdigest()}",
    }
