# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - ENTSO-E
# MAGIC
# MAGIC Reads `bronze.entsoe`, parses the event envelope, validates required
# MAGIC fields, and upserts into `silver.generation`. Malformed rows go to
# MAGIC `quarantine.generation` with a reject_reason.
# MAGIC
# MAGIC **Natural key:** `(country_code, period_start)`. ENTSO-E publishes
# MAGIC one row per (country, hour). TSOs back-publish corrections 2-3 hours
# MAGIC later; MERGE on the natural key naturally absorbs these updates so
# MAGIC Silver always reflects the most recent published value per hour.
# MAGIC
# MAGIC **Timestamp:** ENTSO-E emits clean ISO with offset
# MAGIC (`2026-05-13T22:00:00+00:00`), no special handling needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

BRONZE_TABLE = f"{catalog}.bronze.entsoe"
SILVER_TABLE = f"{catalog}.silver.generation"
QUARANTINE_TABLE = f"{catalog}.quarantine.generation"
NATURAL_KEY = ["country_code", "period_start"]

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
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

GENMIX_ITEM_SCHEMA = StructType(
    [
        StructField("psr_type", StringType(), True),
        StructField("name", StringType(), True),
        StructField("value_mw", DoubleType(), True),
    ]
)

PAYLOAD_SCHEMA = StructType(
    [
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("eic_code", StringType(), True),
        StructField("period_start", StringType(), True),
        StructField("period_end", StringType(), True),
        StructField("resolution", StringType(), True),
        StructField("total_generation_mw", DoubleType(), True),
        StructField("generation_mix", ArrayType(GENMIX_ITEM_SCHEMA), True),
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


def parse_entsoe(bronze_df):
    """Bronze envelope_json -> typed columns ready for validation.

    period_start / period_end come in as ISO-with-offset strings; the default
    to_timestamp parser handles them.
    """
    return bronze_df.withColumn("env", F.from_json(F.col("envelope_json"), ENVELOPE_SCHEMA)).select(
        F.col("env.event_id").alias("event_id"),
        F.col("env.source").alias("source"),
        F.col("env.source_version").alias("source_version"),
        F.to_timestamp(F.col("env.ingested_at")).alias("ingested_at"),
        F.col("env.payload.country_code").alias("country_code"),
        F.col("env.payload.country_name").alias("country_name"),
        F.col("env.payload.eic_code").alias("eic_code"),
        F.to_timestamp(F.col("env.payload.period_start")).alias("period_start"),
        F.to_timestamp(F.col("env.payload.period_end")).alias("period_end"),
        F.col("env.payload.resolution").alias("resolution"),
        F.col("env.payload.total_generation_mw").alias("total_generation_mw"),
        F.col("env.payload.generation_mix").alias("generation_mix"),
        F.col("envelope_json"),
        F.col("kafka_timestamp"),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate

# COMMAND ----------


def validate_entsoe(parsed_df):
    return parsed_df.withColumn(
        "reject_reason",
        F.when(F.col("country_code").isNull(), F.lit("missing country_code"))
        .when(F.col("period_start").isNull(), F.lit("invalid period_start"))
        .when(F.col("period_end").isNull(), F.lit("invalid period_end"))
        .when(F.col("total_generation_mw").isNull(), F.lit("missing total_generation_mw"))
        .when(F.col("total_generation_mw") < 0, F.lit("negative total generation"))
        # Sanity: France max real load is ~100 GW; flag anything > 200 GW
        .when(F.col("total_generation_mw") > 200000, F.lit("unrealistic generation magnitude"))
        .when(F.size(F.col("generation_mix")) == 0, F.lit("empty generation_mix array"))
        .otherwise(F.lit(None).cast("string")),
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

bronze_df = read_bronze(spark, BRONZE_TABLE)
bronze_rows = bronze_df.count()

parsed = parse_entsoe(bronze_df)
parsed_rows = parsed.count()

valid, invalid = split_valid_invalid(parsed, validate_entsoe)
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
      COUNT(DISTINCT country_code) AS distinct_countries,
      MIN(period_start) AS earliest_period,
      MAX(period_start) AS latest_period,
      ROUND(AVG(total_generation_mw), 1) AS avg_total_mw,
      ROUND(MAX(total_generation_mw), 1) AS peak_total_mw
    FROM {SILVER_TABLE}
""")
silver_summary.show(truncate=False)
