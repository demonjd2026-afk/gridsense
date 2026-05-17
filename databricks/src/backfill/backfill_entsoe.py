# Databricks notebook source
# MAGIC %md
# MAGIC # Backfill — ENTSO-E generation (Big-6 EU bidding zones, hourly)
# MAGIC
# MAGIC One-shot historical backfill for ENTSO-E A75 (Actual Generation per
# MAGIC Production Type) across DE, FR, ES, IT, NL, GB. Writes directly to
# MAGIC `bronze.entsoe` in the same envelope shape the live Container App
# MAGIC producer emits, so the existing Silver job MERGEs new rows on its
# MAGIC next run with zero code change.
# MAGIC
# MAGIC ## Why this notebook exists
# MAGIC
# MAGIC Phase 8 (ML forecasting) needs historical generation-mix data joined
# MAGIC with weather and carbon intensity. The live producer was emitting at
# MAGIC 1-hour cadence per country starting ~5 days ago. Backfilling 3 years
# MAGIC of history takes ~25 minutes of compute and gives us a defensible
# MAGIC training window with three full winters and three full summers.
# MAGIC
# MAGIC ## Why we skip Event Hubs
# MAGIC
# MAGIC Identical reasoning to Phase 8.A. Live = streaming, historical =
# MAGIC batch, both land in the same Bronze table. Silver MERGE on natural
# MAGIC key `(country_code, period_start)` handles any overlap.
# MAGIC
# MAGIC ## How this differs from 8.A
# MAGIC
# MAGIC - **XML response**, not JSON. Parsed via `xmltodict`.
# MAGIC - **Per-country API calls.** Six countries × N chunks = many calls.
# MAGIC - **Hourly slicing.** Each chunk covers ~30 days. We have to bucket
# MAGIC   Points into hours ourselves to match the producer's grain.
# MAGIC - **Auth required.** ENTSO-E token from Databricks secret scope.
# MAGIC - **Throttling.** ENTSO-E free tier is 400 req/hour. We sleep 250ms
# MAGIC   between calls (~240 req/min worst case, well under cap).
# MAGIC
# MAGIC ## Source identifier
# MAGIC
# MAGIC Envelopes are tagged `source="entsoe-backfill"` (vs the live
# MAGIC producer's `entsoe`). Lets audit queries distinguish backfill vs
# MAGIC live rows; Silver doesn't care which.
# MAGIC
# MAGIC ## Limitations
# MAGIC
# MAGIC - **TSOs occasionally back-publish corrections.** The live producer
# MAGIC   handles this by MERGE (later row wins for same natural key).
# MAGIC   Backfill of long-stale periods captures the most recently published
# MAGIC   value, which is what we want.
# MAGIC - **Empty hours.** Some hours have no TimeSeries (TSO didn't publish
# MAGIC   or generation was zero). Backfill skips these silently — they
# MAGIC   simply don't appear in Silver.
# MAGIC - **Older PSR codes.** The producer maps 21+ PsrType codes; if we
# MAGIC   encounter an unknown code in historical data, it's labeled
# MAGIC   "Unknown" same as live producer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install xmltodict (kernel restarts after this — must run first)
# MAGIC
# MAGIC `%pip install` triggers a Python kernel restart in Databricks
# MAGIC Serverless, which wipes all Python variables defined before it. We
# MAGIC run it FIRST so nothing gets wiped.

# COMMAND ----------

# MAGIC %pip install xmltodict --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
dbutils.widgets.text("start_date", "2023-05-17")  # 3 years back
dbutils.widgets.text("end_date", "2026-05-17")  # today
dbutils.widgets.text("chunk_days", "30")  # ENTSO-E practical limit
dbutils.widgets.text("countries", "DE,FR,ES,IT,NL,GB")  # comma-separated subset
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
chunk_days = int(dbutils.widgets.get("chunk_days"))
countries_filter = [c.strip() for c in dbutils.widgets.get("countries").split(",")]
dry_run = dbutils.widgets.get("dry_run") == "true"

BRONZE_TABLE = f"{catalog}.bronze.entsoe"
API_URL = "https://web-api.tp.entsoe.eu/api"
SOURCE_NAME = "entsoe-backfill"
SOURCE_VERSION = "v1"
THROTTLE_SECS = 0.25

# ENTSO-E API token from Databricks secret scope
API_TOKEN = dbutils.secrets.get("gridsense-kv", "entsoe-api-token")

print(f"Bronze target:  {BRONZE_TABLE}")
print(f"Date range:     {start_date} → {end_date}")
print(f"Chunk size:     {chunk_days} days")
print(f"Countries:      {countries_filter}")
print(f"Dry run:        {dry_run}")
print(f"API token:      {'set ✓' if API_TOKEN else 'MISSING ✗'}")
assert API_TOKEN, "ENTSO-E API token must be present in gridsense-kv secret scope"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Country and PSR type reference data
# MAGIC
# MAGIC Copied verbatim from the live producer (`producers/entsoe/src/main.py`)
# MAGIC so backfill envelopes match what live produces. If the live producer
# MAGIC adds a country or PSR type, this notebook needs to track it.

# COMMAND ----------

# (country_code, eic_code, friendly_name)
COUNTRIES_ALL: list[tuple[str, str, str]] = [
    ("DE", "10Y1001A1001A83F", "Germany"),
    ("FR", "10YFR-RTE------C", "France"),
    ("ES", "10YES-REE------0", "Spain"),
    ("IT", "10YIT-GRTN-----B", "Italy"),
    ("NL", "10YNL----------L", "Netherlands"),
    ("GB", "10YGB----------A", "Great Britain"),
]
COUNTRIES = [c for c in COUNTRIES_ALL if c[0] in countries_filter]
print(f"Will fetch for: {[c[0] for c in COUNTRIES]}")

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
    "B21": "AC Link",
    "B22": "DC Link",
    "B23": "Substation",
    "B24": "Transformer",
    "B25": "Energy storage",
    "A03": "Mixed",
    "A04": "Generation",
    "A05": "Load",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers — envelope, time math, API fetch, XML slicing
# MAGIC
# MAGIC The slicer is the meat of this notebook. Each API call returns up to
# MAGIC ~30 days of TimeSeries data; we bucket Points into hours and build
# MAGIC one envelope per (country, hour).

# COMMAND ----------

import hashlib
import json
import re
import time
import uuid
from datetime import UTC, datetime, timedelta

import requests
import xmltodict
from pyspark.sql import Row
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def build_envelope(payload: dict, region: str, event_time: str) -> dict:
    """Match the envelope shape produced by gridsense_common.envelope.build_envelope.

    Byte-compatible with the live producer's envelopes. If the live envelope
    schema changes, this must change too.
    """
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return {
        "event_id": str(uuid.uuid4()),
        "source": SOURCE_NAME,
        "source_version": SOURCE_VERSION,
        "ingested_at": datetime.now(UTC).isoformat(),
        "event_time": event_time,
        "region": region,
        "payload": payload,
        "checksum": f"sha256:{hashlib.sha256(body.encode()).hexdigest()}",
    }


def date_chunks(start: str, end: str, days: int):
    """Yield (period_start_str, period_end_str) windows in yyyyMMddHHmm format.

    ENTSO-E expects yyyyMMddHHmm UTC. Each chunk is `days` long; we always
    align to midnight UTC for clean boundaries.
    """
    start_dt = datetime.fromisoformat(start).replace(tzinfo=UTC, hour=0, minute=0, second=0)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=UTC, hour=0, minute=0, second=0)
    cursor = start_dt
    while cursor < end_dt:
        next_cursor = min(cursor + timedelta(days=days), end_dt)
        yield (
            cursor.strftime("%Y%m%d%H%M"),
            next_cursor.strftime("%Y%m%d%H%M"),
        )
        cursor = next_cursor


def parse_iso_z(s: str) -> datetime:
    """Parse '2023-05-17T00:00Z' to a UTC-aware datetime."""
    return datetime.strptime(s, "%Y-%m-%dT%H:%MZ").replace(tzinfo=UTC)


def parse_resolution(resolution: str) -> int:
    """Convert ISO-8601 resolution like 'PT60M', 'PT15M' to seconds.

    ENTSO-E uses PT15M, PT30M, PT60M predominantly. We support all of these
    plus a fallback that extracts any integer minute value.
    """
    if not resolution:
        return 3600  # safe default
    match = re.match(r"PT(\d+)M", resolution)
    if match:
        return int(match.group(1)) * 60
    return 3600


def fetch_chunk_xml(
    eic_code: str, period_start: str, period_end: str, max_retries: int = 3
) -> dict:
    """GET one A75/A16 GL_MarketDocument from ENTSO-E for one country, one chunk.

    Returns the parsed XML as dict via xmltodict. Retries on transient HTTP
    errors with exponential backoff.
    """
    params = {
        "documentType": "A75",
        "processType": "A16",
        "in_Domain": eic_code,
        "periodStart": period_start,
        "periodEnd": period_end,
        "securityToken": API_TOKEN,
    }
    for attempt in range(max_retries):
        try:
            r = requests.get(API_URL, params=params, timeout=60)
            r.raise_for_status()
            return xmltodict.parse(r.text)
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            backoff = 2**attempt
            print(f"    retry {attempt + 1}/{max_retries} after {backoff}s: {e}")
            time.sleep(backoff)
    raise RuntimeError("unreachable")


# COMMAND ----------

# MAGIC %md
# MAGIC ## XML → hourly buckets
# MAGIC
# MAGIC The producer aggregates sub-hourly Points into one MW value per hour
# MAGIC by averaging (not summing — see comment in `producers/entsoe/src/main.py`).
# MAGIC We do the same, but per-hour across the entire chunk.

# COMMAND ----------


def slice_chunk_to_hourly_buckets(parsed: dict) -> dict[datetime, dict[str, list[float]]]:
    """Walk a parsed A75 response and build {hour_start: {psr_type: [MW_values]}}.

    For each TimeSeries (one PsrType), each Period has a start time and a
    resolution. Each Point's `position` (1-indexed) tells us the offset from
    the Period start. We bucket every Point into the hour that contains it.

    Returns:
        Dict keyed by the hour-start datetime (UTC), each containing a dict
        of psr_type -> list of MW values that fall in that hour. The caller
        averages within each list to get the hourly MW per PsrType.
    """
    doc = parsed.get("GL_MarketDocument", {})
    series_raw = doc.get("TimeSeries", [])
    if isinstance(series_raw, dict):
        series_list = [series_raw]
    else:
        series_list = series_raw or []

    buckets: dict[datetime, dict[str, list[float]]] = {}

    for series in series_list:
        psr = series.get("MktPSRType", {}).get("psrType")
        if not psr:
            continue

        # Period can be either dict (single) or list (multiple) — xmltodict
        # convention. For multi-day API chunks, multiple Periods per
        # TimeSeries is common (one per publication window or daily TSO
        # batch). We normalize to a list and iterate.
        period_raw = series.get("Period", [])
        if isinstance(period_raw, dict):
            period_list = [period_raw]
        else:
            period_list = period_raw or []

        for period in period_list:
            time_interval = period.get("timeInterval", {})
            period_start_str = time_interval.get("start")
            resolution = period.get("resolution", "PT60M")
            if not period_start_str:
                continue

            try:
                period_start_dt = datetime.strptime(period_start_str, "%Y-%m-%dT%H:%MZ").replace(
                    tzinfo=UTC
                )
            except ValueError:
                # Skip malformed timestamps rather than crash entire chunk
                continue

            resolution_secs = parse_resolution(resolution)

            points_raw = period.get("Point", [])
            if isinstance(points_raw, dict):
                points_list = [points_raw]
            else:
                points_list = points_raw or []

            for pt in points_list:
                position = pt.get("position")
                quantity = pt.get("quantity")
                if position is None or quantity is None:
                    continue
                try:
                    pos = int(position)
                    qty = float(quantity)
                except (TypeError, ValueError):
                    continue

                # Point N starts at period_start + (N-1) * resolution
                point_dt = period_start_dt + timedelta(seconds=(pos - 1) * resolution_secs)
                hour_start = point_dt.replace(minute=0, second=0, microsecond=0)

                if hour_start not in buckets:
                    buckets[hour_start] = {}
                if psr not in buckets[hour_start]:
                    buckets[hour_start][psr] = []
                buckets[hour_start][psr].append(qty)

    return buckets


def shape_hourly_payload(
    country_code: str,
    country_name: str,
    eic_code: str,
    hour_start: datetime,
    psr_avg_mw: dict[str, float],
) -> dict:
    """Build the per-(country, hour) payload matching live producer shape."""
    generation_mix = [
        {
            "psr_type": psr,
            "name": PSR_TYPE_NAMES.get(psr, "Unknown"),
            "value_mw": round(mw, 2),
        }
        for psr, mw in sorted(psr_avg_mw.items())
    ]
    total_mw = round(sum(psr_avg_mw.values()), 2)
    return {
        "country_code": country_code,
        "country_name": country_name,
        "eic_code": eic_code,
        "period_start": hour_start.isoformat(),
        "period_end": (hour_start + timedelta(hours=1)).isoformat(),
        "resolution": "PT60M",
        "total_generation_mw": total_mw,
        "generation_mix": generation_mix,
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze row schema
# MAGIC
# MAGIC Mirrors `bronze.entsoe` exactly. Same shape as `bronze.carbon_intensity`
# MAGIC (the streaming pipeline lands everything in the same Bronze structure
# MAGIC across topics).

# COMMAND ----------

BRONZE_ROW_SCHEMA = StructType(
    [
        StructField("envelope_json", StringType(), False),
        StructField("kafka_key", StringType(), True),
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("ingested_at_ts", TimestampType(), False),
        StructField("event_date", DateType(), True),
    ]
)


def envelope_to_bronze_row(envelope: dict, hour_start: datetime) -> Row:
    """Build a Bronze row. event_date derives from the event hour, not now()."""
    now = datetime.now(UTC)
    return Row(
        envelope_json=json.dumps(envelope),
        kafka_key=envelope["region"],
        topic="entsoe",
        partition=0,
        offset=0,
        kafka_timestamp=now,
        ingested_at_ts=now,
        event_date=hour_start.date(),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Main pull loop
# MAGIC
# MAGIC For each (chunk, country): fetch XML → slice to hourly buckets →
# MAGIC average per-hour → build envelope → stage Bronze row.

# COMMAND ----------

bronze_rows: list[Row] = []
chunk_list = list(date_chunks(start_date, end_date, chunk_days))
total_chunks = len(chunk_list)
total_calls = total_chunks * len(COUNTRIES)

print(f"Will fetch {total_chunks} chunks × {len(COUNTRIES)} countries = {total_calls} API calls")
print(f"Estimated runtime: ~{total_calls * 4 / 60:.1f} minutes (with throttle)")
print()

start_t = time.time()
events_so_far = 0
empty_chunks = 0
errors = 0

for ci, (chunk_start, chunk_end) in enumerate(chunk_list, 1):
    chunk_events_total = 0

    for country_code, eic_code, country_name in COUNTRIES:
        try:
            parsed = fetch_chunk_xml(eic_code, chunk_start, chunk_end)
        except Exception as e:
            errors += 1
            print(f"  ✗ chunk {ci}/{total_chunks} {country_code} FAILED: {type(e).__name__}: {e}")
            time.sleep(THROTTLE_SECS)
            continue

        buckets = slice_chunk_to_hourly_buckets(parsed)
        if not buckets:
            empty_chunks += 1
            time.sleep(THROTTLE_SECS)
            continue

        for hour_start, psr_values in buckets.items():
            # Average sub-hourly Points within each PsrType for this hour
            psr_avg_mw = {
                psr: sum(values) / len(values) for psr, values in psr_values.items() if values
            }
            if not psr_avg_mw:
                continue

            payload = shape_hourly_payload(
                country_code, country_name, eic_code, hour_start, psr_avg_mw
            )
            envelope = build_envelope(
                payload=payload,
                region=country_code,
                event_time=hour_start.isoformat(),
            )
            bronze_rows.append(envelope_to_bronze_row(envelope, hour_start))
            chunk_events_total += 1

        time.sleep(THROTTLE_SECS)

    events_so_far += chunk_events_total
    elapsed = time.time() - start_t
    if ci % 5 == 0 or ci == total_chunks:
        print(
            f"  chunk {ci:3d}/{total_chunks} | {chunk_start} → {chunk_end} | "
            f"+{chunk_events_total:5d} events | total {events_so_far:7d} | "
            f"empty {empty_chunks:3d} | errors {errors:3d} | "
            f"elapsed {elapsed:6.1f}s"
        )

print()
print(f"✓ fetched {events_so_far:,} events in {time.time() - start_t:.1f}s")
print(f"  empty chunks (no TimeSeries): {empty_chunks}")
print(f"  errored chunks:               {errors}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze

# COMMAND ----------

bronze_df = spark.createDataFrame(bronze_rows, schema=BRONZE_ROW_SCHEMA)

print(f"DataFrame rows: {bronze_df.count():,}")
print(f"Distinct event_dates: {bronze_df.select('event_date').distinct().count():,}")
print(f"Distinct kafka_keys (countries): {bronze_df.select('kafka_key').distinct().count()}")
print()
print("Sample rows (truncated):")
bronze_df.limit(3).show(truncate=80)

if dry_run:
    print("DRY RUN — not writing to Bronze")
else:
    print(f"Writing to {BRONZE_TABLE}…")
    bronze_df.write.mode("append").saveAsTable(BRONZE_TABLE)
    final_count = spark.table(BRONZE_TABLE).count()
    print(f"✓ Bronze total rows after backfill: {final_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps (run manually after this notebook completes)
# MAGIC
# MAGIC 1. Re-run Silver to MERGE the backfill:
# MAGIC    ```
# MAGIC    databricks bundle run silver_entsoe -t dev
# MAGIC    ```
# MAGIC 2. Re-run any Gold facts that depend on ENTSO-E generation:
# MAGIC    ```
# MAGIC    databricks bundle run gold_fact_generation_fuel_hourly -t dev
# MAGIC    databricks bundle run gold_fact_grid_hourly -t dev
# MAGIC    ```
# MAGIC 3. Verify in SQL Editor with `docs/sql/verification/phase8b_backfill_summary.sql`
