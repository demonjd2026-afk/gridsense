# Databricks notebook source
# MAGIC %md
# MAGIC # Backfill — UK Carbon Intensity (regional, 30-min periods)
# MAGIC
# MAGIC One-shot historical backfill for the UK Carbon Intensity API. Writes
# MAGIC directly to `bronze.carbon_intensity` in the same envelope shape the
# MAGIC live producer (Container App) emits, so the existing Silver job picks
# MAGIC up the new rows on its next MERGE without any code change.
# MAGIC
# MAGIC ## Why this notebook exists
# MAGIC
# MAGIC Phase 8 (ML forecasting) needs months of training data. The original
# MAGIC plan was "wait two weeks for accumulation." This backfill collapses
# MAGIC that wait to ten minutes by pulling 3 years of half-hourly historical
# MAGIC data from the public API.
# MAGIC
# MAGIC ## Why we skip Event Hubs
# MAGIC
# MAGIC Live ingestion = streaming path: producer → Event Hubs → Bronze.
# MAGIC This backfill = batch path: API → Bronze (direct).
# MAGIC
# MAGIC Both land in the same table. Silver MERGE on natural key
# MAGIC `(region_code, period_start)` deduplicates if any overlap exists.
# MAGIC Industry-standard lambda split, no special-case downstream code.
# MAGIC
# MAGIC ## Source identifier
# MAGIC
# MAGIC Envelopes are tagged `source="uk-carbon-intensity-backfill"` (vs the
# MAGIC live producer's `uk-carbon-intensity`). This is intentional: lets
# MAGIC audit queries distinguish backfilled rows from live rows. Silver
# MAGIC parses both transparently — neither downstream nor MERGE care which
# MAGIC source string a row has, only the natural key.
# MAGIC
# MAGIC ## Limitations
# MAGIC
# MAGIC - **No actuals at the regional level.** Per NESO's documentation,
# MAGIC   regional carbon intensity is forecast-only. `intensity.actual` will
# MAGIC   be NULL for every backfilled row. National-level actuals exist via
# MAGIC   a different endpoint not used here.
# MAGIC - **API limit:** 14 days per request. Backfilling 3 years = ~78
# MAGIC   sequential calls with polite throttle. Wall-clock ~5-10 minutes.
# MAGIC - **API is rate-tolerant but not unlimited.** We sleep 500ms between
# MAGIC   calls (good citizen).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
dbutils.widgets.text("start_date", "2023-05-17")  # 3 years back from build date
dbutils.widgets.text("end_date", "2026-05-17")  # today
dbutils.widgets.text("chunk_days", "7")  # API max window size
dbutils.widgets.dropdown("dry_run", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
chunk_days = int(dbutils.widgets.get("chunk_days"))
dry_run = dbutils.widgets.get("dry_run") == "true"

BRONZE_TABLE = f"{catalog}.bronze.carbon_intensity"
API_BASE = "https://api.carbonintensity.org.uk/regional/intensity"
SOURCE_NAME = "uk-carbon-intensity-backfill"
SOURCE_VERSION = "v1"
THROTTLE_SECS = 0.5

print(f"Bronze target: {BRONZE_TABLE}")
print(f"Date range:    {start_date} → {end_date}")
print(f"Chunk size:    {chunk_days} days")
print(f"Dry run:       {dry_run}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers — envelope, chunking, API fetch

# COMMAND ----------

import hashlib
import json
import time
import uuid
from datetime import UTC, datetime, timedelta

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

    Critical: keep this byte-compatible with the live producer's envelopes so
    the same Silver parser handles both. If the live envelope schema changes,
    this needs to change too.
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
    """Yield (from_iso, to_iso) windows of `days` length up to but not over `end`."""
    start_dt = datetime.fromisoformat(start).replace(tzinfo=UTC)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=UTC)
    cursor = start_dt
    while cursor < end_dt:
        next_cursor = min(cursor + timedelta(days=days), end_dt)
        yield (
            cursor.strftime("%Y-%m-%dT%H:%MZ"),
            next_cursor.strftime("%Y-%m-%dT%H:%MZ"),
        )
        cursor = next_cursor


def fetch_chunk(from_iso: str, to_iso: str, max_retries: int = 3) -> dict:
    """GET one chunk from the UK CI regional historical endpoint.

    Retries on transient 5xx with exponential backoff. Raises on permanent
    failure so the notebook fails loud rather than silently skipping a window.
    """
    url = f"{API_BASE}/{from_iso}/{to_iso}"
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=30, headers={"Accept": "application/json"})
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                raise
            backoff = 2**attempt
            print(f"  retry {attempt + 1}/{max_retries} after {backoff}s: {e}")
            time.sleep(backoff)
    raise RuntimeError("unreachable")  # appease type checkers


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze row schema
# MAGIC
# MAGIC Mirrors `bronze.carbon_intensity` exactly. Backfill rows are NOT from
# MAGIC Kafka, so kafka_partition/offset are synthetic constants — Silver does
# MAGIC not use these for any business logic.

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


def envelope_to_bronze_row(envelope: dict) -> Row:
    """Build a Bronze row from one envelope dict.

    event_date is derived from the envelope's event_time (the period start),
    NOT from now() — partitioning needs to match what live ingestion does.
    """
    event_time = envelope["event_time"]  # ISO like "2024-01-01T00:00Z"
    event_dt = datetime.strptime(event_time, "%Y-%m-%dT%H:%MZ").replace(tzinfo=UTC)
    now = datetime.now(UTC)

    return Row(
        envelope_json=json.dumps(envelope),
        kafka_key=envelope["region"],
        topic="carbon-intensity",
        partition=0,  # synthetic — backfill rows didn't come via Kafka
        offset=0,  # synthetic
        kafka_timestamp=now,  # when this row was written to Bronze
        ingested_at_ts=now,  # when the backfill produced this row
        event_date=event_dt.date(),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull, transform, and stage into a Spark DataFrame
# MAGIC
# MAGIC We accumulate all chunks into a single Python list, then convert to a
# MAGIC DataFrame and write once. For 3 years × 18 regions × 17,520 periods
# MAGIC ≈ 946K rows this is fine memory-wise (~150 MB peak).

# COMMAND ----------

bronze_rows: list[Row] = []
chunk_list = list(date_chunks(start_date, end_date, chunk_days))
total_chunks = len(chunk_list)

print(f"Will fetch {total_chunks} chunks of {chunk_days} days each")
print(f"Estimated total events: ~{total_chunks * chunk_days * 48 * 18:,}")
print()

start_t = time.time()
events_so_far = 0

for i, (from_iso, to_iso) in enumerate(chunk_list, 1):
    body = fetch_chunk(from_iso, to_iso)
    periods = body.get("data", [])

    chunk_events = 0
    for period in periods:
        period_from = period.get("from")
        period_to = period.get("to")
        if not period_from:
            continue

        for region in period.get("regions", []):
            region_id = region.get("regionid")
            region_short = region.get("shortname")
            region_dno = region.get("dnoregion")
            region_code = region_short or f"region-{region_id}"

            payload = {
                "from": period_from,
                "to": period_to,
                "regionid": region_id,
                "shortname": region_short,
                "dnoregion": region_dno,
                "intensity": region.get("intensity"),
                "generationmix": region.get("generationmix"),
            }
            envelope = build_envelope(payload, region_code, period_from)
            bronze_rows.append(envelope_to_bronze_row(envelope))
            chunk_events += 1

    events_so_far += chunk_events
    elapsed = time.time() - start_t
    if i % 10 == 0 or i == total_chunks:
        print(
            f"  chunk {i:3d}/{total_chunks} | {from_iso} → {to_iso} | "
            f"+{chunk_events:5d} events | total {events_so_far:7d} | "
            f"elapsed {elapsed:6.1f}s"
        )
    time.sleep(THROTTLE_SECS)

print()
print(f"✓ fetched {events_so_far:,} events in {time.time() - start_t:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze
# MAGIC
# MAGIC Append-only. If `dry_run=true`, we just count and show a sample.
# MAGIC Otherwise we write the entire batch in one shot (Bronze accepts any
# MAGIC volume since it's append).

# COMMAND ----------

bronze_df = spark.createDataFrame(bronze_rows, schema=BRONZE_ROW_SCHEMA)

print(f"DataFrame rows: {bronze_df.count():,}")
print(f"Distinct event_dates: {bronze_df.select('event_date').distinct().count()}")
print(f"Distinct kafka_keys (regions): {bronze_df.select('kafka_key').distinct().count()}")
print()
print("Sample rows:")
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
# MAGIC 1. Run `silver_carbon_intensity` job to MERGE Bronze backfill into Silver:
# MAGIC    ```
# MAGIC    databricks bundle run silver_carbon_intensity -t dev
# MAGIC    ```
# MAGIC 2. Run `gold_fact_carbon_intensity_30min` to refresh Gold:
# MAGIC    ```
# MAGIC    databricks bundle run gold_fact_carbon_intensity_30min -t dev
# MAGIC    ```
# MAGIC 3. Verify in SQL Editor with `docs/sql/verification/phase8a_backfill_summary.sql`
