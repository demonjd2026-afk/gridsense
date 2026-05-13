"""ENTSO-E Transparency Platform producer.

Polls ENTSO-E every hour for Actual Generation per Production Type (documentType
A75, processType A16) across the Big 6 European bidding zones and publishes one
event per country to Azure Event Hubs.

ENTSO-E publishes hourly settlement-period data with ~2-3 hour publication lag,
so we poll for the window [now - 4h, now - 1h] and rely on Delta MERGE downstream
to handle late-arriving corrections (TSOs occasionally back-publish revisions).

Auth has two layers:
  - ENTSO-E API token (Key Vault -> env var ENTSOE_API_TOKEN, passed as ?securityToken=)
  - Azure managed identity for Event Hubs OAuth (same path as carbon-intensity/open-meteo)
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import structlog
import xmltodict
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
EH_TOPIC = os.environ.get("EVENTHUB_TOPIC", "entsoe")
API_URL = os.environ.get("ENTSOE_API_URL", "https://web-api.tp.entsoe.eu/api")
API_TOKEN = os.environ.get("ENTSOE_API_TOKEN")  # mounted from Key Vault
POLL_INTERVAL_S = int(os.environ.get("POLL_INTERVAL_S", "3600"))  # 1 hr default
HTTP_TIMEOUT_S = int(os.environ.get("HTTP_TIMEOUT_S", "60"))

SOURCE_NAME = "entsoe"
SOURCE_VERSION = "v1"

# Big 6 European bidding zones with their EIC codes.
# (country_code, eic_code, friendly_name)
# These are the official ENTSO-E control-area codes; do NOT change them
# without checking the ENTSO-E EIC area list:
# https://www.entsoe.eu/data/energy-identification-codes-eic/
COUNTRIES: list[tuple[str, str, str]] = [
    ("DE", "10Y1001A1001A83F", "Germany"),
    ("FR", "10YFR-RTE------C", "France"),
    ("ES", "10YES-REE------0", "Spain"),
    ("IT", "10YIT-GRTN-----B", "Italy"),
    ("NL", "10YNL----------L", "Netherlands"),
    ("GB", "10YGB----------A", "Great Britain"),
]

# PsrType (Production Source Type) human-readable mapping.
# Source: ENTSO-E TP API guide, codelist standard_PsrType.
PSR_TYPE_NAMES: dict[str, str] = {
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B07": "Fossil Oil shale",
    "B08": "Fossil Peat",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and poundage",
    "B12": "Hydro Water Reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
    # B21-B24 are grid-infrastructure types per the ENTSO-E codelist
    # (https://github.com/EnergieID/entsoe-py mappings.py). They should not
    # appear in A75 generation responses, but we include them defensively
    # so the producer never publishes an "Unknown" label.
    "B21": "AC Link",
    "B22": "DC Link",
    "B23": "Substation",
    "B24": "Transformer",
    "B25": "Energy storage",
    # A03-A05 are higher-level aggregations occasionally seen in TimeSeries
    # for split bidding zones (e.g. DE-AT-LU before the 2018 split).
    "A03": "Mixed",
    "A04": "Generation",
    "A05": "Load",
}

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
# Time window helpers
# ============================================================================
def make_period_window() -> tuple[str, str, datetime]:
    """Compute the (periodStart, periodEnd, anchor_hour) for the API query.

    ENTSO-E expects yyyyMMddHHmm in UTC with 1-hour resolution. We query for
    the hour that ended 3 hours ago to give TSOs time to publish - going
    closer to now returns empty TimeSeries about half the time.

    Returns:
        period_start: e.g. "202605130200" (start of target hour)
        period_end:   e.g. "202605130300" (end of target hour)
        anchor:       the datetime corresponding to period_start (UTC, naive H:00)
    """
    now = datetime.now(UTC)
    target_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)
    period_start = target_hour.strftime("%Y%m%d%H%M")
    period_end = (target_hour + timedelta(hours=1)).strftime("%Y%m%d%H%M")
    return period_start, period_end, target_hour


# ============================================================================
# Upstream fetch
# ============================================================================
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
    reraise=True,
)
async def fetch_generation(
    client: httpx.AsyncClient,
    eic_code: str,
    period_start: str,
    period_end: str,
) -> dict[str, Any]:
    """Fetch the A75/A16 GL_MarketDocument for one bidding zone.

    Returns the parsed XML as a dict via xmltodict. The shape is:
      GL_MarketDocument:
        TimeSeries: list[...] | dict  (one per PsrType)
          - MktPSRType:
              psrType: "B14"
          - Period:
              timeInterval: {start, end}
              resolution: "PT60M"
              Point: list[...] | dict
                - position: "1"
                - quantity: "12345.0"
    """
    params = {
        "documentType": "A75",  # Actual generation per type
        "processType": "A16",  # Realised (vs A01 day-ahead, etc.)
        "in_Domain": eic_code,
        "periodStart": period_start,
        "periodEnd": period_end,
        "securityToken": API_TOKEN,
    }
    response = await client.get(API_URL, params=params, timeout=HTTP_TIMEOUT_S)
    response.raise_for_status()
    return xmltodict.parse(response.text)


def extract_generation_mix(parsed: dict[str, Any]) -> dict[str, float]:
    """Reduce an ENTSO-E response dict to {psr_type: total_mw_for_hour}.

    The response has one TimeSeries per psrType. Each TimeSeries contains a
    Period with one or more Points (resolution is PT60M for hourly data, so
    usually one Point per Series). We sum all Points within each Series.

    xmltodict quirk: when there is exactly one child element, it returns a
    dict; multiple children become a list. We normalize both shapes.
    """
    doc = parsed.get("GL_MarketDocument", {})
    series_raw = doc.get("TimeSeries", [])
    if isinstance(series_raw, dict):
        series_list = [series_raw]
    else:
        series_list = series_raw

    mix: dict[str, float] = {}
    for series in series_list:
        psr = series.get("MktPSRType", {}).get("psrType")
        if not psr:
            continue
        period = series.get("Period", {})
        points_raw = period.get("Point", [])
        if isinstance(points_raw, dict):
            points_list = [points_raw]
        else:
            points_list = points_raw

        # ENTSO-E returns sub-hourly Points (typically PT15M = 4 points/hr)
        # within each TimeSeries. Each Point's `quantity` is the average MW
        # for that interval. To get the hourly MW we average the points,
        # NOT sum them (summing produces 4x values for 15-min resolution).
        values: list[float] = []
        for pt in points_list:
            qty = pt.get("quantity")
            if qty is None:
                continue
            try:
                values.append(float(qty))
            except (TypeError, ValueError):
                pass

        if not values:
            continue
        avg_mw = sum(values) / len(values)

        # Accumulate across TimeSeries in case the same psrType appears
        # multiple times (rare but possible for split bidding zones).
        mix[psr] = mix.get(psr, 0.0) + avg_mw

    return mix


def shape_payload(
    country_code: str,
    country_name: str,
    eic_code: str,
    period_start_iso: str,
    period_end_iso: str,
    mix: dict[str, float],
) -> dict[str, Any]:
    """Compose the per-country event payload.

    Includes both the raw psrType->MW mapping AND a human-readable
    generation_mix array of {psr_type, name, value_mw}. The raw form lets
    downstream consumers join cleanly to PsrType reference tables; the
    readable form makes ad-hoc debugging painless.
    """
    generation_mix = [
        {
            "psr_type": psr,
            "name": PSR_TYPE_NAMES.get(psr, "Unknown"),
            "value_mw": round(mw, 2),
        }
        for psr, mw in sorted(mix.items())
    ]
    total_mw = round(sum(mix.values()), 2)
    return {
        "country_code": country_code,
        "country_name": country_name,
        "eic_code": eic_code,
        "period_start": period_start_iso,
        "period_end": period_end_iso,
        "resolution": "PT60M",
        "total_generation_mw": total_mw,
        "generation_mix": generation_mix,
    }


# ============================================================================
# Main loop
# ============================================================================
async def publish_snapshot(
    client: httpx.AsyncClient,
    producer: AIOKafkaProducer,
) -> int:
    """One poll cycle: fetch each country, publish one event per country."""
    period_start, period_end, anchor = make_period_window()
    period_end_dt = anchor + timedelta(hours=1)
    period_start_iso = anchor.isoformat()
    period_end_iso = period_end_dt.isoformat()

    sent = 0
    for country_code, eic, country_name in COUNTRIES:
        try:
            parsed = await fetch_generation(client, eic, period_start, period_end)
            mix = extract_generation_mix(parsed)
            if not mix:
                # Empty response - TSO hasn't published for this hour yet.
                # Not an error; log and move on. Next poll picks it up.
                log.info(
                    "no_data",
                    country=country_code,
                    period_start=period_start,
                )
                continue
            payload = shape_payload(
                country_code,
                country_name,
                eic,
                period_start_iso,
                period_end_iso,
                mix,
            )
            envelope = build_envelope(
                payload=payload,
                region=country_code,
                source_name=SOURCE_NAME,
                source_version=SOURCE_VERSION,
                event_time=period_start_iso,
            )
            payload_bytes = json.dumps(envelope).encode()
            key_bytes = country_code.encode()
            await producer.send_and_wait(EH_TOPIC, value=payload_bytes, key=key_bytes)
            sent += 1
        except Exception as exc:
            log.error(
                "country_failed",
                country=country_code,
                error=str(exc),
                error_type=type(exc).__name__,
            )
    return sent


async def main_loop() -> None:
    if not API_TOKEN:
        log.error("missing_token", message="ENTSOE_API_TOKEN env var is empty")
        sys.exit(1)

    log.info(
        "starting",
        namespace=EH_NAMESPACE,
        topic=EH_TOPIC,
        poll_s=POLL_INTERVAL_S,
        countries=len(COUNTRIES),
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
