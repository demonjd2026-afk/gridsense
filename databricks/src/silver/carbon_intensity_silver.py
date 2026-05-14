# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Carbon Intensity
# MAGIC
# MAGIC Reads `bronze.carbon_intensity`, parses the event envelope, validates
# MAGIC required fields, and upserts into `silver.carbon_intensity`. Malformed
# MAGIC rows go to `quarantine.carbon_intensity` with a reject_reason.
# MAGIC
# MAGIC **Natural key:** `(region_code, period_start)`. The UK Carbon Intensity
# MAGIC API publishes one row per (region, 30-min settlement period); each
# MAGIC settlement period is initially a forecast and gets back-published as
# MAGIC an actual ~2 hours later. MERGE on the natural key naturally handles
# MAGIC this: the actual overwrites the forecast.
# MAGIC
# MAGIC **Timestamp quirk:** Carbon Intensity API emits `2026-05-14T01:30Z`
# MAGIC (no seconds, Z suffix). Spark needs an explicit format string.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

BRONZE_TABLE = f"{catalog}.bronze.carbon_intensity"
SILVER_TABLE = f"{catalog}.silver.carbon_intensity"
QUARANTINE_TABLE = f"{catalog}.quarantine.carbon_intensity"
NATURAL_KEY = ["region_code", "period_start"]

print(f"Bronze:     {BRONZE_TABLE}")
print(f"Silver:     {SILVER_TABLE}")
print(f"Quarantine: {QUARANTINE_TABLE}")

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envelope schema
# MAGIC Explicit schema beats schema inference: it catches drift early and
# MAGIC keeps the Spark plan stable across runs.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

INTENSITY_SCHEMA = StructType(
    [
        StructField("forecast", IntegerType(), True),
        StructField("actual", IntegerType(), True),
        StructField("index", StringType(), True),
    ]
)

GENMIX_ITEM_SCHEMA = StructType(
    [
        StructField("fuel", StringType(), True),
        StructField("perc", DoubleType(), True),
    ]
)

PAYLOAD_SCHEMA = StructType(
    [
        StructField("from", StringType(), True),
        StructField("to", StringType(), True),
        StructField("regionid", IntegerType(), True),
        StructField("shortname", StringType(), True),
        StructField("dnoregion", StringType(), True),
        StructField("intensity", INTENSITY_SCHEMA, True),
        StructField("generationmix", ArrayType(GENMIX_ITEM_SCHEMA), True),
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
# MAGIC Convert the JSON string into typed columns, normalize timestamps.

# COMMAND ----------

# Carbon Intensity timestamp format: "2026-05-14T01:30Z" (no seconds).
# We accept both that and the seconds-bearing variant just in case.
CI_TS_FORMATS = ["yyyy-MM-dd'T'HH:mm'Z'", "yyyy-MM-dd'T'HH:mm:ss'Z'"]


def parse_ci_timestamp(col):
    """Try each known format; first non-null wins."""
    return F.coalesce(*[F.to_timestamp(col, fmt) for fmt in CI_TS_FORMATS])


def parse_carbon_intensity(bronze_df):
    """Bronze envelope_json -> typed columns ready for validation."""
    parsed = bronze_df.withColumn(
        "env", F.from_json(F.col("envelope_json"), ENVELOPE_SCHEMA)
    ).select(
        # Envelope-level
        F.col("env.event_id").alias("event_id"),
        F.col("env.source").alias("source"),
        F.col("env.source_version").alias("source_version"),
        F.to_timestamp(F.col("env.ingested_at")).alias("ingested_at"),
        # Payload-level (the analytical fields)
        F.col("env.payload.regionid").alias("region_id"),
        F.col("env.payload.shortname").alias("region_code"),
        F.col("env.payload.dnoregion").alias("region_name"),
        parse_ci_timestamp(F.col("env.payload.`from`")).alias("period_start"),
        parse_ci_timestamp(F.col("env.payload.`to`")).alias("period_end"),
        F.col("env.payload.intensity.forecast").alias("intensity_forecast"),
        F.col("env.payload.intensity.actual").alias("intensity_actual"),
        F.col("env.payload.intensity.index").alias("intensity_index"),
        F.col("env.payload.generationmix").alias("generation_mix"),
        # Carry the raw envelope JSON forward so we can debug quarantines
        # without going back to Bronze.
        F.col("envelope_json"),
        F.col("kafka_timestamp"),
    )
    return parsed


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate
# MAGIC A row is valid if all of: region_code, period_start, intensity_forecast
# MAGIC are non-null. We treat region_id as nice-to-have. Reasons stack via
# MAGIC `when` chains so the first failing check wins.

# COMMAND ----------


def validate_carbon_intensity(parsed_df):
    return parsed_df.withColumn(
        "reject_reason",
        F.when(F.col("region_code").isNull(), F.lit("missing region_code"))
        .when(F.col("period_start").isNull(), F.lit("invalid period_start timestamp"))
        .when(F.col("period_end").isNull(), F.lit("invalid period_end timestamp"))
        .when(F.col("intensity_forecast").isNull(), F.lit("missing intensity_forecast"))
        .otherwise(F.lit(None).cast("string")),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

bronze_df = read_bronze(spark, BRONZE_TABLE)
bronze_rows = bronze_df.count()

parsed = parse_carbon_intensity(bronze_df)
parsed_rows = parsed.count()

valid, invalid = split_valid_invalid(parsed, validate_carbon_intensity)

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
      COUNT(DISTINCT region_code) AS distinct_regions,
      MIN(period_start) AS earliest_period,
      MAX(period_start) AS latest_period,
      SUM(CASE WHEN intensity_actual IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_actual
    FROM {SILVER_TABLE}
""")
silver_summary.show(truncate=False)
