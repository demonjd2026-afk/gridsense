# Databricks notebook source
# MAGIC %md
# MAGIC # Backfill — Open-Meteo historical forecast (6 EU cities, hourly)
# MAGIC
# MAGIC One-shot historical backfill for Open-Meteo across London, Paris,
# MAGIC Berlin, Madrid, Rome, Amsterdam. Writes directly to `bronze.open_meteo`
# MAGIC in the same envelope shape the live Container App producer emits, so
# MAGIC the existing Silver job MERGEs new rows on its next run with zero code
# MAGIC change.
# MAGIC
# MAGIC ## Why this notebook exists
# MAGIC
# MAGIC `gold.fact_grid_hourly` joins ENTSO-E + UK carbon + weather. After
# MAGIC Phase 8.A (UK carbon backfill) and 8.B (ENTSO-E backfill), the
# MAGIC integrated fact was still stuck at 352 rows because weather data was
# MAGIC only present for the live-data window. This backfill closes that gap.
# MAGIC
# MAGIC ## Why the Historical Forecast API
# MAGIC
# MAGIC Open-Meteo offers two relevant archives:
# MAGIC   - **Historical Forecast API**: archived initial-hours from each
# MAGIC     past Forecast API run. Identical schema and models to the live
# MAGIC     producer. Coverage from ~2021.
# MAGIC   - **ERA5 Archive**: reanalysis data with observation correction.
# MAGIC     More accurate but uses different model output.
# MAGIC
# MAGIC We pick Historical Forecast because:
# MAGIC   1. Schema is byte-identical to live producer — no field renames or
# MAGIC      unit conversions.
# MAGIC   2. Same models means zero distributional shift between training and
# MAGIC      inference. The model sees the same data shape and quality at
# MAGIC      train time as it will at predict time.
# MAGIC   3. Open-Meteo positions it specifically as "the standard dataset for
# MAGIC      training bias-correction and post-processing pipelines."
# MAGIC
# MAGIC ## How this differs from 8.A and 8.B
# MAGIC
# MAGIC - **JSON, not XML.** Way simpler parsing.
# MAGIC - **No auth required.** Free, CC BY 4.0 license.
# MAGIC - **One call per (city, large window).** The API accepts multi-year
# MAGIC   ranges in one call.
# MAGIC - **Hourly grain comes for free.** The API returns parallel time[]
# MAGIC   and variable[] arrays at hourly resolution. We zip them.
# MAGIC
# MAGIC ## Source identifier
# MAGIC
# MAGIC Envelopes are tagged `source="open-meteo-backfill"` (vs the live
# MAGIC producer's `open-meteo`). Audit-distinguishable; Silver doesn't care
# MAGIC which it processes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
dbutils.widgets.text("start_date", "2023-05-17")  # 3 years back
dbutils.widgets.text("end_date", "2026-05-17")  # today
dbutils.widgets.text("cities", "London,Paris,Berlin,Madrid,Rome,Amsterdam")
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
cities_filter = [c.strip() for c in dbutils.widgets.get("cities").split(",")]
dry_run = dbutils.widgets.get("dry_run") == "true"

BRONZE_TABLE = f"{catalog}.bronze.open_meteo"
API_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
SOURCE_NAME = "open-meteo-backfill"
SOURCE_VERSION = "v1"
HOURLY_VARS = "temperature_2m,wind_speed_10m,cloud_cover,shortwave_radiation"
THROTTLE_SECS = 0.5

print(f"Bronze target:  {BRONZE_TABLE}")
print(f"Date range:     {start_date} → {end_date}")
print(f"Cities:         {cities_filter}")
print(f"Dry run:        {dry_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## City reference data
# MAGIC
# MAGIC Copied verbatim from the live producer (`producers/open-meteo/src/main.py`)
# MAGIC so backfill envelopes match. If the live producer adds a city, this
# MAGIC notebook needs to track it.

# COMMAND ----------

# (lat, lon, city_name)
CITIES_ALL: list[tuple[float, float, str]] = [
    (51.5074, -0.1278, "London"),
    (48.8566, 2.3522, "Paris"),
    (52.5200, 13.4050, "Berlin"),
    (40.4168, -3.7038, "Madrid"),
    (41.9028, 12.4964, "Rome"),
    (52.3676, 4.9041, "Amsterdam"),
]
CITIES = [c for c in CITIES_ALL if c[2] in cities_filter]
print(f"Will fetch for: {[c[2] for c in CITIES]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers — envelope, API fetch

# COMMAND ----------

import hashlib
import json
import time
import uuid
from datetime import UTC, datetime

import requests
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

    Byte-compatible with the live producer's envelopes.
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


def fetch_city_history(lat: float, lon: float, start: str, end: str, max_retries: int = 3) -> dict:
    """GET a multi-year window of hourly forecasts for one city.

    The historical-forecast-api accepts very wide date ranges in one call.
    For 3 years × 4 variables, the response is ~26,000 hourly values
    (~2MB JSON). Comfortable in a single request.

    Returns the parsed JSON dict with `hourly.time[]` and parallel variable
    arrays.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start,
        "end_date": end,
        "hourly": HOURLY_VARS,
        "timezone": "UTC",
    }
    for attempt in range(max_retries):
        try:
            r = requests.get(API_URL, params=params, timeout=120)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            backoff = 2**attempt
            print(f"    retry {attempt + 1}/{max_retries} after {backoff}s: {e}")
            time.sleep(backoff)
    raise RuntimeError("unreachable")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Response → envelopes
# MAGIC
# MAGIC Open-Meteo returns parallel arrays. We zip them into per-hour records
# MAGIC matching the live producer's `first_hour_snapshot()` output shape.

# COMMAND ----------


def response_to_hourly_payloads(response: dict, city: str) -> list[tuple[dict, str]]:
    """Walk an API response and produce (payload, event_time_iso) tuples.

    Each returned tuple becomes one envelope and one Bronze row.
    """
    hourly = response.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return []

    temps = hourly.get("temperature_2m", [])
    winds = hourly.get("wind_speed_10m", [])
    clouds = hourly.get("cloud_cover", [])
    solars = hourly.get("shortwave_radiation", [])

    units = response.get("hourly_units", {})
    latitude = response.get("latitude")
    longitude = response.get("longitude")
    elevation = response.get("elevation")

    n = len(times)
    payloads: list[tuple[dict, str]] = []

    for i in range(n):
        time_str = times[i]
        # Skip rows where everything is NULL (rare but possible at the
        # edges of the historical archive).
        temp_val = temps[i] if i < len(temps) else None
        if temp_val is None:
            continue

        payload = {
            "city": city,
            "latitude": latitude,
            "longitude": longitude,
            "elevation": elevation,
            "time": time_str,
            "temperature_2m": temp_val,
            "wind_speed_10m": winds[i] if i < len(winds) else None,
            # cloud_cover comes back as int in live, sometimes float in
            # historical — coerce to int to match the Silver IntegerType
            # cast.
            "cloud_cover": (int(clouds[i]) if i < len(clouds) and clouds[i] is not None else None),
            "shortwave_radiation": (solars[i] if i < len(solars) else None),
            "units": {
                "temperature_2m": units.get("temperature_2m"),
                "wind_speed_10m": units.get("wind_speed_10m"),
                "cloud_cover": units.get("cloud_cover"),
                "shortwave_radiation": units.get("shortwave_radiation"),
            },
        }
        payloads.append((payload, time_str))

    return payloads


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze row schema
# MAGIC
# MAGIC Mirrors `bronze.open_meteo` exactly. Backfill rows are NOT from Kafka,
# MAGIC so partition/offset are synthetic constants — Silver doesn't use them
# MAGIC for any business logic.

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


def envelope_to_bronze_row(envelope: dict, time_str: str) -> Row:
    """Build a Bronze row. event_date derives from the event hour string."""
    # Open-Meteo emits "yyyy-MM-ddTHH:mm" (no seconds, no tz). The first 10
    # chars are the ISO date.
    event_date = datetime.strptime(time_str[:10], "%Y-%m-%d").date()
    now = datetime.now(UTC)
    return Row(
        envelope_json=json.dumps(envelope),
        kafka_key=envelope["region"],
        topic="weather",
        partition=0,
        offset=0,
        kafka_timestamp=now,
        ingested_at_ts=now,
        event_date=event_date,
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Main pull loop
# MAGIC
# MAGIC One API call per city for the full date range. The Historical
# MAGIC Forecast API handles multi-year ranges in a single call.

# COMMAND ----------

bronze_rows: list[Row] = []
total_cities = len(CITIES)

print(f"Will fetch {total_cities} API calls (one per city, full date range)")
print()

start_t = time.time()
events_so_far = 0
errors = 0

for ci, (lat, lon, city) in enumerate(CITIES, 1):
    try:
        response = fetch_city_history(lat, lon, start_date, end_date)
    except Exception as e:
        errors += 1
        print(f"  ✗ {city} FAILED: {type(e).__name__}: {e}")
        time.sleep(THROTTLE_SECS)
        continue

    payloads = response_to_hourly_payloads(response, city)
    city_events = 0
    for payload, time_str in payloads:
        envelope = build_envelope(payload=payload, region=city, event_time=time_str)
        bronze_rows.append(envelope_to_bronze_row(envelope, time_str))
        city_events += 1

    events_so_far += city_events
    elapsed = time.time() - start_t
    print(
        f"  city {ci}/{total_cities} {city:10s} | +{city_events:6d} events | "
        f"total {events_so_far:7d} | elapsed {elapsed:6.1f}s"
    )
    time.sleep(THROTTLE_SECS)

print()
print(f"✓ fetched {events_so_far:,} events in {time.time() - start_t:.1f}s")
print(f"  errored cities: {errors}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze

# COMMAND ----------

bronze_df = spark.createDataFrame(bronze_rows, schema=BRONZE_ROW_SCHEMA)

print(f"DataFrame rows: {bronze_df.count():,}")
print(f"Distinct event_dates: {bronze_df.select('event_date').distinct().count():,}")
print(f"Distinct kafka_keys (cities): {bronze_df.select('kafka_key').distinct().count()}")
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
# MAGIC    databricks bundle run silver_open_meteo -t dev
# MAGIC    ```
# MAGIC 2. Re-run Silver grid_state (3-way join):
# MAGIC    ```
# MAGIC    databricks bundle run silver_grid_state -t dev
# MAGIC    ```
# MAGIC 3. Re-run Gold facts that depend on weather:
# MAGIC    ```
# MAGIC    databricks bundle run gold_fact_grid_hourly -t dev
# MAGIC    ```
# MAGIC 4. Verify in SQL Editor with `docs/sql/verification/phase8c_backfill_summary.sql`
