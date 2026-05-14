# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Open-Meteo
# MAGIC
# MAGIC Reads `bronze.open_meteo`, parses the event envelope, validates
# MAGIC required fields, and upserts into `silver.weather`. Malformed rows
# MAGIC go to `quarantine.weather` with a reject_reason.
# MAGIC
# MAGIC **Natural key:** `(city, time_utc)`. Open-Meteo publishes one row per
# MAGIC (city, hour). The producer polls every 15 min, but each cycle returns
# MAGIC the current hour''s forecast, so multiple polls within the same hour
# MAGIC produce duplicate rows in Bronze. MERGE on (city, time_utc) collapses
# MAGIC these to one row per hour, with the latest value winning.
# MAGIC
# MAGIC **Timestamp quirk:** Open-Meteo emits `2026-05-14T00:00` (no timezone
# MAGIC suffix). The producer queries with `timezone=UTC`, so the value IS
# MAGIC UTC; we append `+00:00` explicitly so Spark casts unambiguously.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

BRONZE_TABLE = f"{catalog}.bronze.open_meteo"
SILVER_TABLE = f"{catalog}.silver.weather"
QUARANTINE_TABLE = f"{catalog}.quarantine.weather"
NATURAL_KEY = ["city", "time_utc"]

print(f"Bronze:     {BRONZE_TABLE}")
print(f"Silver:     {SILVER_TABLE}")
print(f"Quarantine: {QUARANTINE_TABLE}")

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envelope schema

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

PAYLOAD_SCHEMA = StructType(
    [
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("elevation", DoubleType(), True),
        StructField("time", StringType(), True),
        StructField("temperature_2m", DoubleType(), True),
        StructField("wind_speed_10m", DoubleType(), True),
        StructField("cloud_cover", IntegerType(), True),
        StructField("shortwave_radiation", DoubleType(), True),
        # units substruct intentionally omitted - it does not vary per row
        # and column metadata is a better home for it (Phase 7 dim table).
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("source_version", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("region", StringType(), True),
        StructField("payload", PAYLOAD_SCHEMA, True),
        StructField("checksum", StringType(), True),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse + cast

# COMMAND ----------


def parse_open_meteo(bronze_df):
    """Bronze envelope_json -> typed columns ready for validation."""
    return bronze_df.withColumn("env", F.from_json(F.col("envelope_json"), ENVELOPE_SCHEMA)).select(
        F.col("env.event_id").alias("event_id"),
        F.col("env.source").alias("source"),
        F.col("env.source_version").alias("source_version"),
        F.to_timestamp(F.col("env.ingested_at")).alias("ingested_at"),
        F.col("env.payload.city").alias("city"),
        F.col("env.payload.latitude").alias("latitude"),
        F.col("env.payload.longitude").alias("longitude"),
        F.col("env.payload.elevation").alias("elevation"),
        # Producer fetches with timezone=UTC and emits "yyyy-MM-ddTHH:mm"
        # (no seconds, no tz). Spark's default to_timestamp parser
        # requires seconds, so we append ":00+00:00" to produce a
        # fully-qualified ISO-8601 string. Doing it inline (vs an
        # explicit format string) avoids the backslash-quote escaping
        # mess in Python notebooks.
        F.to_timestamp(F.concat(F.col("env.payload.time"), F.lit(":00+00:00"))).alias("time_utc"),
        F.col("env.payload.temperature_2m").alias("temperature_c"),
        F.col("env.payload.wind_speed_10m").alias("wind_speed_kmh"),
        F.col("env.payload.cloud_cover").alias("cloud_cover_pct"),
        F.col("env.payload.shortwave_radiation").alias("solar_radiation_wm2"),
        F.col("envelope_json"),
        F.col("kafka_timestamp"),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------


def validate_open_meteo(parsed_df):
    return parsed_df.withColumn(
        "reject_reason",
        F.when(F.col("city").isNull(), F.lit("missing city"))
        .when(F.col("time_utc").isNull(), F.lit("invalid time_utc"))
        .when(F.col("temperature_c").isNull(), F.lit("missing temperature_2m"))
        # Sanity bounds. Real weather sometimes hits extremes; pick wide.
        .when(F.col("temperature_c") < -60, F.lit("temperature below physical minimum"))
        .when(F.col("temperature_c") > 60, F.lit("temperature above physical maximum"))
        .otherwise(F.lit(None).cast("string")),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

bronze_df = read_bronze(spark, BRONZE_TABLE)
bronze_rows = bronze_df.count()

parsed = parse_open_meteo(bronze_df)
parsed_rows = parsed.count()

valid, invalid = split_valid_invalid(parsed, validate_open_meteo)
valid_rows = valid.count()
invalid_rows = invalid.count()

merge_into_silver(spark, valid, SILVER_TABLE, NATURAL_KEY)
quarantined = write_quarantine(spark, invalid, QUARANTINE_TABLE)


log_counts(
    print,
    bronze_rows=bronze_rows,
    parsed_rows=parsed_rows,
    valid_rows=valid_rows,
    invalid_rows=invalid_rows,
)
print(f"merged_into_silver={valid_rows}  quarantined_appended={quarantined}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

silver_summary = spark.sql(f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT city) AS distinct_cities,
      MIN(time_utc) AS earliest_hour,
      MAX(time_utc) AS latest_hour
    FROM {SILVER_TABLE}
""")
silver_summary.show(truncate=False)
