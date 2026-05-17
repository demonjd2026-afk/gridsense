# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - fact_grid_hourly
# MAGIC
# MAGIC Integrated hourly fact: one row per (country_code, hour_utc).
# MAGIC Built from silver.grid_state (which already joins generation +
# MAGIC weather + UK carbon at hourly grain) plus fuel-mix-derived measures
# MAGIC from joining the embedded generation_mix array to dim_fuel_type.
# MAGIC
# MAGIC ## Why this fact exists
# MAGIC
# MAGIC The other two gold facts have narrow grains:
# MAGIC - fact_generation_fuel_hourly: (country, hour, fuel)
# MAGIC - fact_carbon_intensity_30min: (region, period) — UK only
# MAGIC
# MAGIC fact_grid_hourly is the integrated (country, hour) view. It's the
# MAGIC feature table that feeds Phase 8 ML forecasting: every row is a
# MAGIC complete picture of one country's grid at one hour, with weather
# MAGIC drivers + generation outputs + carbon outcome.
# MAGIC
# MAGIC ## Two share definitions
# MAGIC
# MAGIC We compute both renewable_share_pct and low_carbon_share_pct because
# MAGIC they answer different questions:
# MAGIC - renewable_share_pct uses dim_fuel_type.is_renewable
# MAGIC   (excludes nuclear, INCLUDES biomass — standard IEA/Ember definition)
# MAGIC - low_carbon_share_pct uses dim_fuel_type.is_low_carbon
# MAGIC   (INCLUDES nuclear, excludes biomass — IPCC AR5 lifecycle definition)
# MAGIC
# MAGIC The dim correctly classifies biomass as renewable but NOT low-carbon
# MAGIC (combustion releases present-day CO2 even though the feedstock regrows
# MAGIC over decades). Many naive carbon dashboards conflate these. Carrying
# MAGIC both columns lets downstream queries pick the right one.
# MAGIC
# MAGIC ## CO2 calculation independence
# MAGIC
# MAGIC estimated_lifecycle_gco2_per_kwh is recomputed here from silver.grid_state
# MAGIC directly rather than queried from fact_generation_fuel_hourly. This is
# MAGIC intentional: the two facts cross-verify each other. Same unit chain as
# MAGIC fact_generation_fuel_hourly (post Phase 7.C unit fix):
# MAGIC   value_mw      MW per fuel
# MAGIC   x 1000        kWh/MWh conversion
# MAGIC   x gco2_per_kwh g/kWh lifecycle factor
# MAGIC   = grams CO2 per hour
# MAGIC
# MAGIC **Natural key:** (country_code, hour_utc)

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
catalog = dbutils.widgets.get("catalog")

GRID_STATE_TABLE = f"{catalog}.silver.grid_state"
DIM_COUNTRY = f"{catalog}.gold.dim_country"
DIM_FUEL = f"{catalog}.gold.dim_fuel_type"
DIM_TIME = f"{catalog}.gold.dim_time"
TARGET_TABLE = f"{catalog}.gold.fact_grid_hourly"
NATURAL_KEY = ["country_code", "hour_utc"]

# COMMAND ----------

# MAGIC %run ../silver/common

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read grid_state and explode the embedded generation_mix
# MAGIC
# MAGIC silver.grid_state is already a 3-way join: generation + weather + UK
# MAGIC carbon, keyed on (country_code, hour_utc). The generation_mix column
# MAGIC is an array<struct> we need to explode for fuel-mix-derived measures.

# COMMAND ----------

grid = spark.table(GRID_STATE_TABLE).alias("g")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute fuel-mix-derived measures
# MAGIC
# MAGIC Explode generation_mix, join to dim_fuel_type to attach the
# MAGIC is_renewable / is_low_carbon flags and typical_gco2_per_kwh,
# MAGIC then aggregate back to (country_code, hour_utc) grain.

# COMMAND ----------

exploded = (
    grid.select(
        F.col("g.hour_utc"),
        F.col("g.country_code"),
        F.explode("g.generation_mix").alias("fuel_entry"),
    )
    .select(
        F.col("hour_utc"),
        F.col("country_code"),
        F.col("fuel_entry.psr_type").alias("psr_type"),
        F.col("fuel_entry.value_mw").alias("value_mw"),
    )
    .alias("e")
)

# COMMAND ----------

# Join to dim_fuel_type via (source_taxonomy='entsoe', source_code=psr_type)
dim_fuel = (
    spark.table(DIM_FUEL)
    .filter(F.col("source_taxonomy") == "entsoe")
    .select(
        F.col("source_code").alias("dim_psr_type"),
        F.col("is_renewable"),
        F.col("is_low_carbon"),
        F.col("typical_gco2_per_kwh"),
    )
    .alias("f")
)

with_fuel = exploded.join(
    dim_fuel,
    F.col("e.psr_type") == F.col("f.dim_psr_type"),
    how="inner",  # inner: a missing psr_type is a data-quality issue,
    # fail loud rather than producing NULL flags silently.
)

# COMMAND ----------

# Aggregate fuel-level rows back to (country_code, hour_utc) grain
agg_measures = (
    with_fuel.groupBy(
        F.col("e.country_code").alias("country_code"),
        F.col("e.hour_utc").alias("hour_utc"),
    )
    .agg(
        F.sum(F.when(F.col("f.is_renewable"), F.col("e.value_mw")).otherwise(0)).alias(
            "renewable_generation_mw"
        ),
        F.sum(F.when(F.col("f.is_low_carbon"), F.col("e.value_mw")).otherwise(0)).alias(
            "low_carbon_generation_mw"
        ),
        F.sum(F.col("e.value_mw") * F.lit(1000) * F.col("f.typical_gco2_per_kwh")).alias(
            "estimated_lifecycle_gco2_per_hour"
        ),
    )
    .alias("a")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join back into grid_state for the final fact shape

# COMMAND ----------

combined = (
    grid.join(
        agg_measures,
        (F.col("g.country_code") == F.col("a.country_code"))
        & (F.col("g.hour_utc") == F.col("a.hour_utc")),
        how="left",
    )
    .select(
        # Natural key columns (kept as plain values, FKs added below)
        F.col("g.hour_utc").alias("hour_utc"),
        F.col("g.country_code").alias("country_code"),
        # Weather measures (pass-through)
        F.col("g.temperature_c").alias("temperature_c"),
        F.col("g.wind_speed_kmh").alias("wind_speed_kmh"),
        F.col("g.cloud_cover_pct").alias("cloud_cover_pct"),
        F.col("g.solar_radiation_wm2").alias("solar_radiation_wm2"),
        # Generation measures
        F.col("g.total_generation_mw").alias("total_generation_mw"),
        F.col("a.renewable_generation_mw").alias("renewable_generation_mw"),
        F.col("a.low_carbon_generation_mw").alias("low_carbon_generation_mw"),
        F.when(
            F.col("g.total_generation_mw") > 0,
            F.round(
                100.0 * F.col("a.renewable_generation_mw") / F.col("g.total_generation_mw"),
                2,
            ),
        ).alias("renewable_share_pct"),
        F.when(
            F.col("g.total_generation_mw") > 0,
            F.round(
                100.0 * F.col("a.low_carbon_generation_mw") / F.col("g.total_generation_mw"),
                2,
            ),
        ).alias("low_carbon_share_pct"),
        # Carbon measures
        # gco2_per_kwh = total_grams_per_hour / total_kwh_per_hour
        # total_kwh_per_hour = total_generation_mw * 1000
        F.when(
            F.col("g.total_generation_mw") > 0,
            F.round(
                F.col("a.estimated_lifecycle_gco2_per_hour")
                / (F.col("g.total_generation_mw") * F.lit(1000)),
                2,
            ),
        ).alias("estimated_lifecycle_gco2_per_kwh"),
        F.col("a.estimated_lifecycle_gco2_per_hour").alias("estimated_lifecycle_gco2_per_hour"),
        F.col("g.uk_carbon_intensity_forecast").alias("uk_carbon_intensity_forecast"),
        # Metadata
        F.col("g.ingested_at").alias("ingested_at"),
    )
    .alias("c")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add surrogate FKs
# MAGIC
# MAGIC Same convention as fact_generation_fuel_hourly:
# MAGIC - time_key = yyyyMMddHH as BIGINT (matches dim_time.time_key)
# MAGIC - country_key = country_code (the natural key in dim_country)

# COMMAND ----------

with_keys = combined.select(
    F.expr("CAST(date_format(c.hour_utc, 'yyyyMMddHH') AS BIGINT)").alias("time_key"),
    F.col("c.country_code").alias("country_key"),
    F.col("c.hour_utc").alias("hour_utc"),
    F.col("c.country_code").alias("country_code"),
    F.col("c.temperature_c").alias("temperature_c"),
    F.col("c.wind_speed_kmh").alias("wind_speed_kmh"),
    F.col("c.cloud_cover_pct").alias("cloud_cover_pct"),
    F.col("c.solar_radiation_wm2").alias("solar_radiation_wm2"),
    F.col("c.total_generation_mw").alias("total_generation_mw"),
    F.col("c.renewable_generation_mw").alias("renewable_generation_mw"),
    F.col("c.low_carbon_generation_mw").alias("low_carbon_generation_mw"),
    F.col("c.renewable_share_pct").alias("renewable_share_pct"),
    F.col("c.low_carbon_share_pct").alias("low_carbon_share_pct"),
    F.col("c.estimated_lifecycle_gco2_per_kwh").alias("estimated_lifecycle_gco2_per_kwh"),
    F.col("c.estimated_lifecycle_gco2_per_hour").alias("estimated_lifecycle_gco2_per_hour"),
    F.col("c.uk_carbon_intensity_forecast").alias("uk_carbon_intensity_forecast"),
    F.col("c.ingested_at").alias("ingested_at"),
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
      COUNT(DISTINCT country_code, hour_utc) AS distinct_natural_keys,
      COUNT(DISTINCT country_code) AS countries,
      COUNT(DISTINCT hour_utc) AS hours,
      MIN(hour_utc) AS earliest,
      MAX(hour_utc) AS latest,
      ROUND(AVG(renewable_share_pct), 1) AS avg_renewable_pct,
      ROUND(AVG(low_carbon_share_pct), 1) AS avg_low_carbon_pct,
      ROUND(AVG(estimated_lifecycle_gco2_per_kwh), 0) AS avg_gco2_per_kwh,
      SUM(CASE WHEN uk_carbon_intensity_forecast IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_uk_carbon
    FROM {TARGET_TABLE}
""")
summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample: latest-hour snapshot per country

# COMMAND ----------

latest = spark.sql(f"""
    SELECT
      country_code,
      ROUND(total_generation_mw, 0) AS total_mw,
      renewable_share_pct,
      low_carbon_share_pct,
      estimated_lifecycle_gco2_per_kwh AS gco2_per_kwh,
      ROUND(estimated_lifecycle_gco2_per_hour / 1e6, 0) AS tons_co2_per_hour,
      uk_carbon_intensity_forecast
    FROM {TARGET_TABLE}
    WHERE hour_utc = (SELECT MAX(hour_utc) FROM {TARGET_TABLE})
    ORDER BY estimated_lifecycle_gco2_per_kwh DESC
""")
latest.show(50, truncate=False)
