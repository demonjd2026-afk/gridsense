# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - fact_generation_fuel_hourly
# MAGIC
# MAGIC Generation broken down by fuel type, one row per (country, hour, fuel).
# MAGIC The first true star-schema fact in this project: joins to all three
# MAGIC dims (time, country, fuel) via surrogate keys.
# MAGIC
# MAGIC The fuel join uses (source_taxonomy="entsoe", source_code=psr_type)
# MAGIC to map silver.generation.generation_mix entries to their canonical
# MAGIC fuel_key in dim_fuel_type. This is the abstraction that makes the
# MAGIC unified fuel taxonomy pay off downstream: Power BI queries "what was
# MAGIC the renewable share for FR last hour?" become a single 4-way star
# MAGIC join with no CASE WHEN ladders.
# MAGIC
# MAGIC We also compute estimated_gco2_per_hour = value_mw * typical_gco2_per_kwh,
# MAGIC giving a lifecycle-carbon view that complements live grid-intensity
# MAGIC data from silver.carbon_intensity. (Live intensity captures the
# MAGIC actual mix at a moment; lifecycle estimates capture the typical
# MAGIC emission profile of each technology.)
# MAGIC
# MAGIC **Natural key:** (country_code, hour_utc, fuel_key)

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

GENERATION_TABLE = f"{catalog}.silver.generation"
DIM_COUNTRY = f"{catalog}.gold.dim_country"
DIM_FUEL = f"{catalog}.gold.dim_fuel_type"
DIM_TIME = f"{catalog}.gold.dim_time"
TARGET_TABLE = f"{catalog}.gold.fact_generation_fuel_hourly"
NATURAL_KEY = ["country_code", "hour_utc", "fuel_key"]

print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %run ../silver/common

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode generation_mix and join to dims

# COMMAND ----------

# Step 1: explode generation_mix array -> one row per (country, hour, fuel)
exploded = (
    spark.table(GENERATION_TABLE)
    .select(
        F.col("period_start").alias("hour_utc"),
        F.col("country_code"),
        F.col("ingested_at"),
        F.explode("generation_mix").alias("fuel_entry"),
    )
    .select(
        "hour_utc",
        "country_code",
        "ingested_at",
        F.col("fuel_entry.psr_type").alias("psr_type"),
        F.col("fuel_entry.value_mw").alias("value_mw"),
    )
    .alias("g")
)

# COMMAND ----------

# Step 2: join to dim_fuel_type via (source_taxonomy='entsoe', source_code=psr_type)
# Aliases everywhere - Spark Connect is strict about ambiguity.
dim_fuel = (
    spark.table(DIM_FUEL)
    .filter(F.col("source_taxonomy") == "entsoe")
    .select(
        F.col("source_code").alias("dim_psr_type"),
        F.col("fuel_key"),
        F.col("display_name").alias("fuel_display_name"),
        F.col("fuel_category"),
        F.col("is_renewable"),
        F.col("is_low_carbon"),
        F.col("typical_gco2_per_kwh"),
    )
    .alias("f")
)

with_fuel = exploded.join(
    dim_fuel,
    F.col("g.psr_type") == F.col("f.dim_psr_type"),
    how="inner",  # inner: if a psr_type is not in dim_fuel, that is a real
    # data-quality issue and we want it to FAIL LOUDLY here
    # rather than silently producing rows with NULL fuel_key.
)

# COMMAND ----------

# Step 3: surrogate keys for time and country.
# time_key = yyyyMMddHH as BIGINT (matches dim_time.time_key)
# country_key = country_code (already the natural key in dim_country)

with_keys = with_fuel.select(
    F.expr("CAST(date_format(g.hour_utc, 'yyyyMMddHH') AS BIGINT)").alias("time_key"),
    F.col("g.country_code").alias("country_key"),
    F.col("f.fuel_key").alias("fuel_key"),
    F.col("g.hour_utc").alias("hour_utc"),
    F.col("g.country_code").alias("country_code"),
    F.col("g.psr_type").alias("source_psr_type"),
    F.col("f.fuel_display_name").alias("fuel_display_name"),
    F.col("f.fuel_category").alias("fuel_category"),
    F.col("f.is_renewable").alias("is_renewable"),
    F.col("f.is_low_carbon").alias("is_low_carbon"),
    F.col("g.value_mw").alias("value_mw"),
    F.col("f.typical_gco2_per_kwh").alias("typical_gco2_per_kwh"),
    # estimated_gco2_per_hour, in grams CO2 equivalent per hour:
    #   value_mw       MW averaged over the hour
    #   x 1000         kWh/MWh conversion (since typical_gco2_per_kwh is per kWh)
    #   x typical      g CO2 / kWh (IPCC AR5 lifecycle factor from dim_fuel_type)
    # Result is grams/hour. Divide by 1e6 downstream for tons/hour.
    (F.col("g.value_mw") * F.lit(1000) * F.col("f.typical_gco2_per_kwh")).alias(
        "estimated_gco2_per_hour"
    ),
    F.col("g.ingested_at").alias("ingested_at"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into the fact table

# COMMAND ----------

merge_into_silver(spark, with_keys, TARGET_TABLE, NATURAL_KEY)
print(f"merged into {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT country_code, hour_utc, fuel_key) AS distinct_natural_keys,
      COUNT(DISTINCT country_code) AS countries,
      COUNT(DISTINCT fuel_key) AS fuels_seen,
      COUNT(DISTINCT hour_utc) AS hours,
      MIN(hour_utc) AS earliest,
      MAX(hour_utc) AS latest,
      ROUND(SUM(value_mw), 0) AS total_mw_summed,
      ROUND(SUM(estimated_gco2_per_hour) / 1e12, 2) AS total_megatons_co2_eq
    FROM {TARGET_TABLE}
""")
summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample: latest-hour carbon emissions by country and fuel

# COMMAND ----------

latest = spark.sql(f"""
    SELECT
      country_code,
      fuel_category,
      ROUND(SUM(value_mw), 0) AS total_mw,
      ROUND(SUM(estimated_gco2_per_hour) / 1e6, 1) AS tons_co2_eq_per_hr
    FROM {TARGET_TABLE}
    WHERE hour_utc = (SELECT MAX(hour_utc) FROM {TARGET_TABLE})
    GROUP BY country_code, fuel_category
    ORDER BY country_code, tons_co2_eq_per_hr DESC
""")
latest.show(50, truncate=False)
