"""Open-Meteo weather producer.

Polls the Open-Meteo forecast API every 15 minutes for 6 European cities
and publishes one event per city to Azure Event Hubs.

Open-Meteo is free, requires no token, and returns hourly forecasts for
temperature, wind speed, solar radiation, and cloud cover. These are the
upstream signals for our renewable-generation forecasting models in
Phase 9 (ML).

Auth: Managed identity via DefaultAzureCredential -> OAuth bearer token ->
Kafka SASL OAUTHBEARER. No connection strings or access keys anywhere.

Run modes:
  - Production: deployed as an Azure Container App with the producers
    User-Assigned Managed Identity attached.
  - Local dev: `uv run python src/main.py` after `az login`.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from typing import Any

import httpx
import structlog
from aiokafka import AIOKafkaProducer
from azure.identity import DefaultAzureCredential
from gridsense_common import build_envelope, make_producer
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ============================================================================
# Configuration
# ============================================================================
EH_NAMESPACE = os.environ.get("EVENTHUB_NAMESPACE", "evhns-gridsense-dev")
EH_TOPIC = os.environ.get("EVENTHUB_TOPIC", "weather")
API_URL = os.environ.get(
    "OPEN_METEO_URL",
    "https://api.open-meteo.com/v1/forecast",
)
POLL_INTERVAL_S = int(os.environ.get("POLL_INTERVAL_S", "900"))  # 15 min
HTTP_TIMEOUT_S = int(os.environ.get("HTTP_TIMEOUT_S", "30"))

SOURCE_NAME = "open-meteo"
SOURCE_VERSION = "v1"

# Six European cities. Coordinates are (lat, lon, name).
# These are roughly the largest electricity-demand centers in their countries,
# which matters because we'll join weather to grid load in the Gold layer.
CITIES: list[tuple[float, float, str]] = [
    (51.5074, -0.1278, "London"),
    (48.8566, 2.3522, "Paris"),
    (52.5200, 13.4050, "Berlin"),
    (40.4168, -3.7038, "Madrid"),
    (41.9028, 12.4964, "Rome"),
    (52.3676, 4.9041, "Amsterdam"),
]

# Variables we want per city. shortwave_radiation is the standard proxy for
# solar PV generation potential; wind_speed_10m feeds wind-farm forecasts.
HOURLY_VARS = "temperature_2m,wind_speed_10m,cloud_cover,shortwave_radiation"

# ============================================================================
# Logging
# ============================================================================
structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.JSONRenderer(),
    ]
)
log = structlog.get_logger()

shutdown_event = asyncio.Event()


# ============================================================================
# Upstream fetch
# ============================================================================
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    reraise=True,
)
async def fetch_city(
    client: httpx.AsyncClient,
    lat: float,
    lon: float,
) -> dict[str, Any]:
    """Fetch the current-hour forecast for one city.

    We request `forecast_days=1` and pick the first hourly row — that's the
    current hour's forecast. Open-Meteo refreshes hourly so this is the
    freshest signal available.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": HOURLY_VARS,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    response = await client.get(API_URL, params=params, timeout=HTTP_TIMEOUT_S)
    response.raise_for_status()
    return response.json()


def first_hour_snapshot(api_response: dict[str, Any], city: str) -> dict[str, Any]:
    """Reduce an Open-Meteo hourly response to a single point-in-time payload.

    The hourly response contains parallel arrays under `hourly`: time[],
    temperature_2m[], etc. With forecast_days=1 the response covers 24 hours
    starting at today\'s UTC midnight, so times[0] is "today 00:00 UTC".

    We want the **current** hour, not the start of the day. We pick the index
    whose timestamp is the latest one <= now_utc. This way the producer
    publishes a meaningful "now" snapshot regardless of when it polls.
    """
    from datetime import UTC, datetime

    hourly = api_response.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        # Defensive: should never happen on a 200 response, but if Open-Meteo
        # ever returns an empty array we don\'t want to crash the producer.
        raise ValueError(f"empty hourly forecast for {city}")

    # Find the index of the current hour. Open-Meteo emits "yyyy-MM-ddTHH:mm"
    # strings (no tz; documented UTC since we passed timezone=UTC).
    now_iso_hour = datetime.now(UTC).strftime("%Y-%m-%dT%H:00")
    try:
        idx = times.index(now_iso_hour)
    except ValueError:
        # If for some reason the current hour is not in the array (clock skew,
        # API just rolled over) fall back to the latest available index that
        # is <= now. This is safer than failing.
        idx = max(
            (i for i, t in enumerate(times) if t <= now_iso_hour),
            default=0,
        )

    return {
        "city": city,
        "latitude": api_response.get("latitude"),
        "longitude": api_response.get("longitude"),
        "elevation": api_response.get("elevation"),
        "time": times[idx],
        "temperature_2m": hourly.get("temperature_2m", [None])[idx],
        "wind_speed_10m": hourly.get("wind_speed_10m", [None])[idx],
        "cloud_cover": hourly.get("cloud_cover", [None])[idx],
        "shortwave_radiation": hourly.get("shortwave_radiation", [None])[idx],
        "units": {
            "temperature_2m": api_response.get("hourly_units", {}).get("temperature_2m"),
            "wind_speed_10m": api_response.get("hourly_units", {}).get("wind_speed_10m"),
            "cloud_cover": api_response.get("hourly_units", {}).get("cloud_cover"),
            "shortwave_radiation": api_response.get("hourly_units", {}).get("shortwave_radiation"),
        },
    }


# ============================================================================
# Main loop
# ============================================================================
async def publish_snapshot(
    client: httpx.AsyncClient,
    producer: AIOKafkaProducer,
) -> int:
    """One poll cycle: fetch all cities, publish one event per city."""
    sent = 0
    for lat, lon, city in CITIES:
        try:
            api_response = await fetch_city(client, lat, lon)
            payload = first_hour_snapshot(api_response, city)
            envelope = build_envelope(
                payload=payload,
                region=city,
                source_name=SOURCE_NAME,
                source_version=SOURCE_VERSION,
            )
            payload_bytes = json.dumps(envelope).encode()
            key_bytes = city.encode()
            await producer.send_and_wait(EH_TOPIC, value=payload_bytes, key=key_bytes)
            sent += 1
        except Exception as exc:
            # One city failing shouldn't break the whole cycle.
            log.error(
                "city_failed",
                city=city,
                error=str(exc),
                error_type=type(exc).__name__,
            )
    return sent


async def main_loop() -> None:
    log.info(
        "starting",
        namespace=EH_NAMESPACE,
        topic=EH_TOPIC,
        poll_s=POLL_INTERVAL_S,
        cities=len(CITIES),
    )

    credential = DefaultAzureCredential()
    producer = await make_producer(credential, EH_NAMESPACE)

    try:
        async with httpx.AsyncClient(http2=False) as client:
            while not shutdown_event.is_set():
                try:
                    sent = await publish_snapshot(client, producer)
                    log.info("published", events=sent)
                except Exception as exc:
                    log.error(
                        "poll_failed",
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )

                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=POLL_INTERVAL_S)
                except TimeoutError:
                    pass
    finally:
        log.info("shutting_down")
        await producer.stop()
        credential.close()
        log.info("shutdown_complete")


def install_signal_handlers() -> None:
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
        sys.exit(0)
