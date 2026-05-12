"""UK Carbon Intensity producer.

Polls the UK National Grid ESO Carbon Intensity API every 5 minutes and
publishes one event per region per settlement period to Azure Event Hubs.

The Carbon Intensity API is free, requires no token, and returns regional
carbon intensity (gCO2/kWh) for the 14 UK Distribution Network Operators.

Auth: Managed identity via DefaultAzureCredential -> OAuth bearer token ->
Kafka SASL OAUTHBEARER. No connection strings or access keys anywhere.

Run modes:
  - Production: deployed as an Azure Container App with the producers
    User-Assigned Managed Identity attached, env vars from container spec.
  - Local dev: `uv run python src/main.py` after `az login` (uses your
    Azure CLI credentials via DefaultAzureCredential's fallback chain).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import ssl
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.abc import AbstractTokenProvider
from azure.identity import DefaultAzureCredential
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ============================================================================
# Configuration (from environment; defaults are safe-ish for local dev)
# ============================================================================
EH_NAMESPACE = os.environ.get("EVENTHUB_NAMESPACE", "evhns-gridsense-dev")
EH_TOPIC = os.environ.get("EVENTHUB_TOPIC", "carbon-intensity")
API_URL = os.environ.get(
    "CARBON_INTENSITY_URL",
    "https://api.carbonintensity.org.uk/regional",
)
POLL_INTERVAL_S = int(os.environ.get("POLL_INTERVAL_S", "300"))  # 5 min default
HTTP_TIMEOUT_S = int(os.environ.get("HTTP_TIMEOUT_S", "30"))

SOURCE_NAME = "uk-carbon-intensity"
SOURCE_VERSION = "v1"

# ============================================================================
# Logging setup (structured JSON; plays well with Log Analytics)
# ============================================================================
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

# Shutdown coordination: set by SIGTERM/SIGINT handlers, awaited by main loop
shutdown_event = asyncio.Event()


# ============================================================================
# Event envelope
# ============================================================================
def build_envelope(payload: dict[str, Any], region: str) -> dict[str, Any]:
    """Wrap a raw API payload in our canonical event envelope.

    The envelope adds:
      - event_id    : globally-unique UUID for dedup
      - source      : which producer published this
      - ingested_at : when we (the producer) generated this event
      - event_time  : when the upstream measurement was actually for
      - region      : routing key for partitioning
      - checksum    : sha256 of the canonical payload for tamper detection

    Keeping the original payload nested under `payload` means downstream
    consumers can always recover the raw source data without us locking
    them into our schema choices.
    """
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return {
        "event_id": str(uuid.uuid4()),
        "source": SOURCE_NAME,
        "source_version": SOURCE_VERSION,
        "ingested_at": datetime.now(UTC).isoformat(),
        "event_time": payload.get("from"),
        "region": region,
        "payload": payload,
        "checksum": f"sha256:{hashlib.sha256(body.encode()).hexdigest()}",
    }


# ============================================================================
# Upstream API fetch (with retries on transient failures)
# ============================================================================
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    reraise=True,
)
async def fetch_regional(client: httpx.AsyncClient) -> dict[str, Any]:
    """Fetch the current regional carbon intensity snapshot.

    Returns the top-level period dict:
      {
        "from": "2026-05-12T15:30Z",
        "to":   "2026-05-12T16:00Z",
        "regions": [
          {
            "regionid": 1,
            "shortname": "North Scotland",
            "intensity": {"forecast": 220, "index": "moderate"},
            "generationmix": [...]
          },
          ...
        ]
      }
    """
    response = await client.get(API_URL, timeout=HTTP_TIMEOUT_S)
    response.raise_for_status()
    body = response.json()
    return body["data"][0]


# ============================================================================
# Token provider for aiokafka SASL OAUTHBEARER
# ============================================================================
class AzureADTokenProvider(AbstractTokenProvider):
    """aiokafka OAuth bearer token provider backed by Azure managed identity.

    aiokafka requires an async `token()` returning a str. The Azure Identity
    SDK's sync `get_token()` is the most reliable variant (the .aio one has
    known compatibility issues in some environments), so we wrap it in
    `run_in_executor` to keep the event loop unblocked.
    """

    def __init__(self, credential: DefaultAzureCredential) -> None:
        self._credential = credential

    def _get_token_sync(self) -> str:
        access_token = self._credential.get_token(
            f"https://{EH_NAMESPACE}.servicebus.windows.net/.default"
        )
        return access_token.token

    async def token(self) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._get_token_sync)


async def make_producer(credential: DefaultAzureCredential) -> AIOKafkaProducer:
    """Construct an aiokafka producer wired for Event Hubs.

    Event Hubs Standard tier exposes a Kafka 1.0+ surface at port 9093 with
    SASL_SSL + OAUTHBEARER. Our managed identity gets an Azure AD token via
    the token provider class; aiokafka uses it as the SASL bearer.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=f"{EH_NAMESPACE}.servicebus.windows.net:9093",
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=AzureADTokenProvider(credential),
        ssl_context=ssl.create_default_context(),
        linger_ms=200,
        acks="all",
        request_timeout_ms=30000,
    )
    await producer.start()
    return producer


# ============================================================================
# Main producer loop
# ============================================================================
async def publish_snapshot(
    client: httpx.AsyncClient,
    producer: AIOKafkaProducer,
) -> int:
    """One poll cycle: fetch upstream, publish one event per region.

    Returns the number of events published in this cycle.
    """
    period = await fetch_regional(client)
    period_from = period.get("from")
    period_to = period.get("to")
    sent = 0

    for region in period.get("regions", []):
        region_code = region.get("shortname", f"region-{region.get('regionid')}")
        # Compose a per-region payload that includes the period window.
        # This is what downstream consumers actually want: one row per
        # (region, settlement period).
        payload = {
            "from": period_from,
            "to": period_to,
            "regionid": region.get("regionid"),
            "shortname": region.get("shortname"),
            "dnoregion": region.get("dnoregion"),
            "intensity": region.get("intensity"),
            "generationmix": region.get("generationmix"),
        }
        envelope = build_envelope(payload, region_code)
        payload_bytes = json.dumps(envelope).encode()
        key_bytes = region_code.encode()
        await producer.send_and_wait(EH_TOPIC, value=payload_bytes, key=key_bytes)
        sent += 1

    return sent


async def main_loop() -> None:
    """Top-level lifecycle: connect, poll forever, shutdown gracefully."""
    log.info("starting", namespace=EH_NAMESPACE, topic=EH_TOPIC, poll_s=POLL_INTERVAL_S)

    credential = DefaultAzureCredential()
    producer = await make_producer(credential)

    try:
        async with httpx.AsyncClient(http2=False) as client:
            while not shutdown_event.is_set():
                try:
                    sent = await publish_snapshot(client, producer)
                    log.info("published", events=sent)
                except Exception as exc:
                    # Don't crash the producer on transient upstream failures.
                    # The retry decorator on fetch_regional handles network
                    # blips; this catch is for everything else (parsing
                    # errors, rare Kafka send failures, etc.).
                    log.error("poll_failed", error=str(exc), error_type=type(exc).__name__)

                # Sleep until next poll OR shutdown, whichever comes first.
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=POLL_INTERVAL_S)
                except TimeoutError:
                    pass  # Normal: time to poll again.
    finally:
        log.info("shutting_down")
        await producer.stop()
        credential.close()
        log.info("shutdown_complete")


def install_signal_handlers() -> None:
    """Catch SIGTERM (from Container Apps) and SIGINT (Ctrl+C) for clean exit."""
    loop = asyncio.get_running_loop()

    def trigger_shutdown(signame: str) -> None:
        log.info("signal_received", signal=signame)
        shutdown_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, trigger_shutdown, sig.name)


async def main() -> None:
    install_signal_handlers()
    await main_loop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Should never reach here because we trap SIGINT, but belt + braces.
        sys.exit(0)
