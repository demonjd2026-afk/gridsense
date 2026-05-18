# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 8.D.1 — Feature engineering for carbon forecasting
# MAGIC
# MAGIC Reads `gold.fact_grid_hourly` and produces a feature-engineered view
# MAGIC `gold.feature_carbon_forecast` ready for ML training (Phase 8.D.2).
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC 1. **Calendar features** — hour-of-day, day-of-week, month, is_weekend
# MAGIC 2. **Lag features** — carbon intensity 1h, 24h, and 168h (1 week) ago
# MAGIC 3. **Rolling stats** — 24h trailing mean of carbon and temperature
# MAGIC 4. **Target column** — `target_t24h` = carbon intensity 24 hours ahead
# MAGIC
# MAGIC All windowed operations partition by `country_code` and order by
# MAGIC `hour_utc` so lags and rolling stats stay within a country's own history.
# MAGIC
# MAGIC ## Why a separate feature table (not just in the training notebook)?
# MAGIC
# MAGIC Three reasons:
# MAGIC
# MAGIC 1. **Idempotency.** Feature computation is deterministic; training is
# MAGIC    stochastic. Separating them means re-training doesn't recompute
# MAGIC    features (and feature drift is detectable as a table-diff).
# MAGIC 2. **Inference reuse.** At inference time the agent needs the same
# MAGIC    feature transformations. A materialized feature table is the
# MAGIC    single source of truth for both train and predict.
# MAGIC 3. **Lakehouse hygiene.** Following the Databricks Feature Store
# MAGIC    pattern even though we don't use the Feature Store API directly —
# MAGIC    Delta table + materialized view is the same shape.
# MAGIC
# MAGIC ## Why predict t+24h specifically?
# MAGIC
# MAGIC "What will the grid look like tomorrow?" is the natural framing for
# MAGIC carbon-aware workload scheduling — the agent's existing use case.
# MAGIC A 24-hour horizon matches when:
# MAGIC   - Batch jobs are typically scheduled
# MAGIC   - Industrial demand-response signals fire
# MAGIC   - EV charging is rescheduled
# MAGIC
# MAGIC One target column = one model = simpler MLflow pipeline. We can
# MAGIC extend to multi-horizon (t+1h, t+6h, t+12h, t+24h) later if needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
dbutils.widgets.dropdown("mode", "create_or_replace", ["create_or_replace", "incremental"])
catalog = dbutils.widgets.get("catalog")
mode = dbutils.widgets.get("mode")

SOURCE_TABLE = f"{catalog}.gold.fact_grid_hourly"
FEATURE_TABLE = f"{catalog}.gold.feature_carbon_forecast"

print(f"Source:        {SOURCE_TABLE}")
print(f"Feature table: {FEATURE_TABLE}")
print(f"Mode:          {mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source row count baseline

# COMMAND ----------

src_count = spark.table(SOURCE_TABLE).count()
print(f"Source row count: {src_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build features
# MAGIC
# MAGIC One window expression per category (lag, rolling, target). All
# MAGIC partitioned by `country_code` so each country's series is treated
# MAGIC as independent.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Window over each country's hourly time series.
country_window = Window.partitionBy("country_code").orderBy("hour_utc")

# Trailing 24h window: 24 rows preceding the current row (exclusive of current).
trailing_24h = Window.partitionBy("country_code").orderBy("hour_utc").rowsBetween(-24, -1)

source_df = spark.table(SOURCE_TABLE)

features_df = (
    source_df
    # Calendar features (deterministic, no window)
    .withColumn("hour_of_day", F.hour("hour_utc"))
    .withColumn("day_of_week", F.dayofweek("hour_utc"))  # 1=Sun..7=Sat
    .withColumn("month", F.month("hour_utc"))
    .withColumn(
        "is_weekend",
        F.col("day_of_week").isin(1, 7),  # Sun or Sat
    )
    # Lag features
    .withColumn(
        "carbon_lag_1h",
        F.lag("estimated_lifecycle_gco2_per_kwh", 1).over(country_window),
    )
    .withColumn(
        "carbon_lag_24h",
        F.lag("estimated_lifecycle_gco2_per_kwh", 24).over(country_window),
    )
    .withColumn(
        "carbon_lag_168h",
        F.lag("estimated_lifecycle_gco2_per_kwh", 168).over(country_window),
    )
    # Rolling stats (trailing 24h, exclusive of current row)
    .withColumn(
        "carbon_rolling_24h_mean",
        F.avg("estimated_lifecycle_gco2_per_kwh").over(trailing_24h),
    )
    .withColumn(
        "temp_rolling_24h_mean",
        F.avg("temperature_c").over(trailing_24h),
    )
    # Target: carbon intensity 24 hours from now
    .withColumn(
        "target_t24h",
        F.lead("estimated_lifecycle_gco2_per_kwh", 24).over(country_window),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select final feature schema
# MAGIC
# MAGIC Drop rows where any required feature or target is NULL. This happens
# MAGIC at the boundaries:
# MAGIC - First week of each country: `carbon_lag_168h` is NULL
# MAGIC - Last day of each country:    `target_t24h` is NULL

# COMMAND ----------

final_df = features_df.select(
    # Identity
    F.col("hour_utc"),
    F.col("country_code"),
    # Categorical (LightGBM will handle string categoricals natively)
    # Numeric features — current grid state
    F.col("temperature_c"),
    F.col("wind_speed_kmh"),
    F.col("cloud_cover_pct"),
    F.col("solar_radiation_wm2"),
    F.col("total_generation_mw"),
    F.col("renewable_share_pct"),
    F.col("low_carbon_share_pct"),
    F.col("estimated_lifecycle_gco2_per_kwh").alias("carbon_current"),
    # Calendar
    F.col("hour_of_day"),
    F.col("day_of_week"),
    F.col("month"),
    F.col("is_weekend"),
    # Lags
    F.col("carbon_lag_1h"),
    F.col("carbon_lag_24h"),
    F.col("carbon_lag_168h"),
    # Rolling
    F.col("carbon_rolling_24h_mean"),
    F.col("temp_rolling_24h_mean"),
    # Target
    F.col("target_t24h"),
).filter(
    # Drop boundary rows where lag or target is NULL
    F.col("carbon_lag_168h").isNotNull()
    & F.col("target_t24h").isNotNull()
    & F.col("carbon_rolling_24h_mean").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick sanity checks before writing

# COMMAND ----------

final_count = final_df.count()
print(f"Feature rows (after dropping NULL boundary rows): {final_count:,}")

# Per-country counts — sanity check coverage
print("\nRows per country:")
final_df.groupBy("country_code").count().orderBy("country_code").show()

# Date span
print("Date range:")
final_df.select(F.min("hour_utc").alias("earliest"), F.max("hour_utc").alias("latest")).show(
    truncate=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write feature table

# COMMAND ----------

if mode == "create_or_replace":
    print(f"Writing {final_count:,} rows to {FEATURE_TABLE} (overwrite)…")
    final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(FEATURE_TABLE)
else:
    # Incremental mode: append only new rows past the latest hour_utc already
    # in the feature table. (Useful for future scheduled refreshes.)
    print(f"Incremental mode: appending new rows to {FEATURE_TABLE}…")
    existing_max = spark.sql(f"SELECT MAX(hour_utc) AS max_hour FROM {FEATURE_TABLE}").collect()[0][
        "max_hour"
    ]
    if existing_max is None:
        print("  Feature table is empty; doing a full overwrite instead.")
        final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            FEATURE_TABLE
        )
    else:
        new_rows = final_df.filter(F.col("hour_utc") > F.lit(existing_max))
        new_count = new_rows.count()
        print(f"  Appending {new_count:,} new rows past {existing_max}")
        new_rows.write.mode("append").saveAsTable(FEATURE_TABLE)

# Final stats
final_total = spark.table(FEATURE_TABLE).count()
print(f"\n✓ Feature table now has {final_total:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC 1. Verify with `docs/sql/verification/phase8d1_features_summary.sql`
# MAGIC 2. Proceed to Phase 8.D.2 training notebook:
# MAGIC    ```
# MAGIC    databricks bundle run ml_train_carbon_forecast -t dev
# MAGIC    ```
