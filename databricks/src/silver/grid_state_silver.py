# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - Grid State (3-way join)
# MAGIC
# MAGIC The integration artifact. Joins the three per-source Silver tables
# MAGIC into one hourly snapshot per country:
# MAGIC
# MAGIC ```
# MAGIC silver.generation  (country x hour)
# MAGIC   LEFT JOIN silver.country_dim     (country -> capital_city)
# MAGIC   LEFT JOIN silver.weather         (city x hour, via capital_city)
# MAGIC   LEFT JOIN silver.carbon_intensity (GB-only national-level aggregated to hourly)
# MAGIC ```
# MAGIC
# MAGIC **Natural key:** `(country_code, hour_utc)`. Same dedup-before-MERGE
# MAGIC pattern as the per-source Silvers (in common.py).
# MAGIC
# MAGIC **Why LEFT joins from generation:** generation is our spine because
# MAGIC it is the only source covering all 6 countries hourly. Weather is
# MAGIC nullable when the city has not published the matching hour yet.
# MAGIC Carbon intensity is GB-only by design (the regional UK API has no
# MAGIC equivalent for other countries; that is Phase 7 work).

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

GENERATION_TABLE = f"{catalog}.silver.generation"
WEATHER_TABLE = f"{catalog}.silver.weather"
CARBON_TABLE = f"{catalog}.silver.carbon_intensity"
COUNTRY_DIM_TABLE = f"{catalog}.silver.country_dim"
TARGET_TABLE = f"{catalog}.silver.grid_state"
NATURAL_KEY = ["country_code", "hour_utc"]

print(f"Target: {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: generation (the spine)

# COMMAND ----------

generation = spark.table(GENERATION_TABLE).select(
    F.col("period_start").alias("hour_utc"),
    "country_code",
    "country_name",
    "total_generation_mw",
    "generation_mix",
    F.col("ingested_at").alias("gen_ingested_at"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: country dim (the bridge)

# COMMAND ----------

country_dim = spark.table(COUNTRY_DIM_TABLE).select("country_code", "capital_city")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: weather (joined via capital city)

# COMMAND ----------

weather = spark.table(WEATHER_TABLE).select(
    F.col("city").alias("capital_city"),
    F.col("time_utc").alias("weather_hour_utc"),
    F.col("temperature_c"),
    F.col("wind_speed_kmh"),
    F.col("cloud_cover_pct"),
    F.col("solar_radiation_wm2"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source: UK carbon intensity (aggregated to hourly, GB only)
# MAGIC The regional Carbon Intensity API publishes 30-min settlement
# MAGIC periods. We aggregate the GB-level national row (regionid=18,
# MAGIC shortname='GB') to hourly by averaging the two half-hours.

# COMMAND ----------

uk_carbon_hourly = (
    spark.table(CARBON_TABLE)
    .filter(F.col("region_code") == "GB")
    .groupBy(F.date_trunc("hour", F.col("period_start")).alias("hour_utc"))
    .agg(
        F.avg("intensity_forecast").alias("uk_carbon_intensity_forecast"),
        # Pick any one of the two index labels in the hour. They are
        # categorical (low/moderate/high) and almost always identical
        # within an hour.
        F.first("intensity_index", ignorenulls=True).alias("uk_carbon_intensity_index"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join all four

# COMMAND ----------

# Alias every source so column references stay unambiguous after joins.
# Spark Connect is stricter than classic Spark about this; without aliases
# we get AMBIGUOUS_REFERENCE on shared column names like capital_city.
g = generation.alias("g")
d = country_dim.alias("d")
w = weather.alias("w")
c = uk_carbon_hourly.alias("c")

joined = (
    g.join(d, F.col("g.country_code") == F.col("d.country_code"), how="left")
    .join(
        w,
        (F.col("d.capital_city") == F.col("w.capital_city"))
        & (F.col("g.hour_utc") == F.col("w.weather_hour_utc")),
        how="left",
    )
    .join(
        c,
        (F.col("g.country_code") == F.lit("GB")) & (F.col("g.hour_utc") == F.col("c.hour_utc")),
        how="left",
    )
    .select(
        F.col("g.hour_utc").alias("hour_utc"),
        F.col("g.country_code").alias("country_code"),
        F.col("g.country_name").alias("country_name"),
        F.col("w.temperature_c").alias("temperature_c"),
        F.col("w.wind_speed_kmh").alias("wind_speed_kmh"),
        F.col("w.cloud_cover_pct").alias("cloud_cover_pct"),
        F.col("w.solar_radiation_wm2").alias("solar_radiation_wm2"),
        F.col("g.total_generation_mw").alias("total_generation_mw"),
        F.col("g.generation_mix").alias("generation_mix"),
        F.col("c.uk_carbon_intensity_forecast").alias("uk_carbon_intensity_forecast"),
        F.col("c.uk_carbon_intensity_index").alias("uk_carbon_intensity_index"),
        F.col("g.gen_ingested_at").alias("ingested_at"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE into silver.grid_state
# MAGIC Reuses the deduped-MERGE primitive from common.py.

# COMMAND ----------

merge_into_silver(spark, joined, TARGET_TABLE, NATURAL_KEY)

print(f"merged into {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT country_code, hour_utc) AS distinct_natural_keys,
      COUNT(DISTINCT country_code) AS countries,
      MIN(hour_utc) AS earliest,
      MAX(hour_utc) AS latest,
      SUM(CASE WHEN temperature_c IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_weather,
      SUM(CASE WHEN uk_carbon_intensity_forecast IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_uk_carbon
    FROM {TARGET_TABLE}
""")
summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample: most recent hour, all countries

# COMMAND ----------

latest = spark.sql(f"""
    SELECT
      hour_utc,
      country_code,
      ROUND(temperature_c, 1) AS temp_c,
      ROUND(wind_speed_kmh, 1) AS wind_kmh,
      cloud_cover_pct,
      ROUND(solar_radiation_wm2, 0) AS solar_wm2,
      ROUND(total_generation_mw, 0) AS gen_mw,
      uk_carbon_intensity_forecast AS uk_co2_gpkwh
    FROM {TARGET_TABLE}
    WHERE hour_utc = (SELECT MAX(hour_utc) FROM {TARGET_TABLE})
    ORDER BY country_code
""")
latest.show(truncate=False)
