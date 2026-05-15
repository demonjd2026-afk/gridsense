# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - fact_carbon_intensity_30min
# MAGIC
# MAGIC UK carbon intensity at 30-min settlement-period grain. One row per
# MAGIC (region_id, period_start). Sources silver.carbon_intensity (UK Carbon
# MAGIC Intensity API), joins to dim_uk_region for region attributes.
# MAGIC
# MAGIC **Two-fact design.** This fact and fact_generation_fuel_hourly answer
# MAGIC complementary questions:
# MAGIC
# MAGIC   - fact_generation_fuel_hourly: "what fuels generated how much MW in
# MAGIC     country X at hour Y, and what's the lifecycle CO2 of that mix?"
# MAGIC     Lifecycle = IPCC AR5 per-technology averages applied to MW.
# MAGIC
# MAGIC   - fact_carbon_intensity_30min: "what's the actual measured grid
# MAGIC     carbon intensity (gCO2/kWh) in UK region X at 30-min interval Y?"
# MAGIC     This is the live grid mix, captured by the UK system operator.
# MAGIC
# MAGIC Phase 8 ML can compare lifecycle vs live intensity to flag windows
# MAGIC where the actual mix is cleaner/dirtier than the typical mix.
# MAGIC
# MAGIC **Forecast vs actual.** The API emits each period twice: first as a
# MAGIC forecast (intensity_actual = null), then ~2 hours later as an actual
# MAGIC (intensity_actual populated). source_type discriminates. The
# MAGIC merge_into_silver helper handles the re-emission case (dedupe on
# MAGIC natural key by latest ingested_at, then MERGE).
# MAGIC
# MAGIC **Natural key:** (region_id, period_start)

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

CARBON_INTENSITY_TABLE = f"{catalog}.silver.carbon_intensity"
DIM_UK_REGION = f"{catalog}.gold.dim_uk_region"
DIM_COUNTRY = f"{catalog}.gold.dim_country"
TARGET_TABLE = f"{catalog}.gold.fact_carbon_intensity_30min"
NATURAL_KEY = ["region_id", "period_start"]

print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %run ../silver/common

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver and join to dim_uk_region

# COMMAND ----------

silver_df = spark.table(CARBON_INTENSITY_TABLE).alias("s")

dim_region = (
    spark.table(DIM_UK_REGION)
    .select(
        F.col("region_id").alias("dim_region_id"),
        F.col("region_code").alias("dim_region_code"),
        F.col("region_name").alias("dim_region_name"),
        F.col("region_type"),
        F.col("country_code"),
    )
    .alias("r")
)

# Inner join: if Silver has a region_id not in the dim, that's a real
# data-quality issue (a new DNO region was added?) - fail loudly here
# rather than silently producing rows with NULL region attributes.
joined = silver_df.join(
    dim_region,
    F.col("s.region_id") == F.col("r.dim_region_id"),
    how="inner",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project to Gold shape

# COMMAND ----------

projected = joined.select(
    # time_key = yyyyMMddHH as BIGINT (matches dim_time grain; 2 fact rows
    # share one time_key, distinguished by half_hour).
    F.expr("CAST(date_format(s.period_start, 'yyyyMMddHH') AS BIGINT)").alias("time_key"),
    F.col("s.region_id").alias("region_id"),
    F.col("r.country_code").alias("country_key"),
    # Denormalized timestamps for direct query (no time dim join needed)
    F.col("s.period_start").alias("period_start"),
    F.col("s.period_end").alias("period_end"),
    F.minute(F.col("s.period_start")).cast("smallint").alias("half_hour"),
    # Denormalized region attrs (matches fact_generation_fuel_hourly pattern)
    F.col("r.dim_region_code").alias("region_code"),
    F.col("r.dim_region_name").alias("region_name"),
    F.col("r.region_type").alias("region_type"),
    # Measures
    F.col("s.intensity_forecast").alias("intensity_forecast"),
    F.col("s.intensity_actual").alias("intensity_actual"),
    F.col("s.intensity_index").alias("intensity_index"),
    # Quality / lineage
    F.when(F.col("s.intensity_actual").isNotNull(), F.lit("actual"))
    .otherwise(F.lit("forecast"))
    .alias("source_type"),
    F.col("s.intensity_actual").isNotNull().alias("has_actual"),
    (F.col("s.intensity_forecast") - F.col("s.intensity_actual")).alias("forecast_minus_actual"),
    # Mix snapshot (denormalized; small array, tightly bound to the row)
    F.col("s.generation_mix").alias("generation_mix"),
    # Lineage
    F.col("s.source").alias("source"),
    F.col("s.ingested_at").alias("ingested_at"),
    F.col("s.event_id").alias("source_event_id"),
)

# Sanity: half_hour must be exactly 0 or 30 (API publishes on the half).
# Half-hour values outside (0, 30) indicate a Silver parse issue.
bad_half = projected.filter(~F.col("half_hour").isin(0, 30)).count()
if bad_half > 0:
    raise ValueError(f"{bad_half} rows have half_hour values outside (0, 30). Inspect Silver.")

print(f"Gold rows ready for MERGE: {projected.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into the fact table

# COMMAND ----------

merge_into_silver(spark, projected, TARGET_TABLE, NATURAL_KEY)
print(f"merged into {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
      COUNT(*)                                                       AS row_count,
      COUNT(DISTINCT region_id, period_start)                        AS distinct_natural_keys,
      COUNT(DISTINCT region_id)                                      AS regions,
      MIN(period_start)                                              AS earliest,
      MAX(period_start)                                              AS latest,
      SUM(CASE WHEN source_type = 'forecast' THEN 1 ELSE 0 END)      AS forecast_rows,
      SUM(CASE WHEN source_type = 'actual'   THEN 1 ELSE 0 END)      AS actual_rows,
      SUM(CASE WHEN intensity_forecast IS NULL THEN 1 ELSE 0 END)    AS null_forecast,
      SUM(CASE WHEN time_key IS NULL THEN 1 ELSE 0 END)              AS null_time_key
    FROM {TARGET_TABLE}
""")
summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample: latest-period intensity ranking across UK regions

# COMMAND ----------

latest = spark.sql(f"""
    SELECT
      region_name,
      region_type,
      period_start,
      intensity_forecast,
      intensity_index,
      source_type
    FROM {TARGET_TABLE}
    WHERE period_start = (SELECT MAX(period_start) FROM {TARGET_TABLE})
    ORDER BY intensity_forecast DESC
""")
latest.show(truncate=False)
