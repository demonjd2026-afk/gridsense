# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 8.D.3 — Inference: carbon forecast predictions
# MAGIC
# MAGIC Loads the registered LightGBM model (Phase 8.D.2) from Unity Catalog,
# MAGIC generates 24-hour-ahead carbon intensity predictions per country, and
# MAGIC writes results to `gold.fact_carbon_forecast`.
# MAGIC
# MAGIC ## What this notebook does
# MAGIC
# MAGIC 1. Loads `dbw_gridsense_dev.ml.carbon_forecast_lgb` (latest version)
# MAGIC 2. Reads recent features from `gold.feature_carbon_forecast`
# MAGIC    (default: last 7 days for richer agent data; configurable)
# MAGIC 3. Runs inference per row → predicted carbon at `hour_utc + 24h`
# MAGIC 4. MERGEs results into `gold.fact_carbon_forecast` on natural key
# MAGIC    `(country_code, base_hour_utc)` — re-running is idempotent
# MAGIC
# MAGIC ## Why MERGE and not append
# MAGIC
# MAGIC Predictions get refreshed when the model is retrained or when feature
# MAGIC values get back-published. MERGE ensures the latest prediction
# MAGIC for a given (country, base_hour) wins, no duplicate forecast rows.
# MAGIC
# MAGIC ## Why 7 days of history (and not just current hour)
# MAGIC
# MAGIC Lets the agent answer questions like:
# MAGIC   - "What did the model predict for yesterday's grid?"
# MAGIC   - "How has the forecast changed over the past week?"
# MAGIC Cost: ~840 rows (5 countries × 168 hours). Trivial compute.
# MAGIC
# MAGIC ## How the agent will use the output
# MAGIC
# MAGIC Phase 8.D.4 adds a new tool `get_carbon_forecast(country_code,
# MAGIC hours_ahead=24)` that reads from this table. The agent gets one
# MAGIC SQL query away from a real ML prediction.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
dbutils.widgets.text("model_name", "carbon_forecast_lgb")
dbutils.widgets.text("model_version", "latest")  # or e.g. "1", "2"
dbutils.widgets.text("inference_days", "7")  # how many days back to predict for
dbutils.widgets.dropdown("mode", "merge", ["merge", "overwrite"])

catalog = dbutils.widgets.get("catalog")
model_name_short = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")
inference_days = int(dbutils.widgets.get("inference_days"))
mode = dbutils.widgets.get("mode")

FEATURE_TABLE = f"{catalog}.gold.feature_carbon_forecast"
FORECAST_TABLE = f"{catalog}.gold.fact_carbon_forecast"
REGISTERED_MODEL_NAME = f"{catalog}.ml.{model_name_short}"
HORIZON_H = 24

print(f"Feature table:    {FEATURE_TABLE}")
print(f"Forecast table:   {FORECAST_TABLE}")
print(f"Model:            {REGISTERED_MODEL_NAME} (version: {model_version})")
print(f"Inference days:   {inference_days}")
print(f"Horizon:          {HORIZON_H} hours")
print(f"Write mode:       {mode}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install LightGBM
# MAGIC
# MAGIC The model artifact requires lightgbm to deserialize.

# COMMAND ----------

# MAGIC %pip install lightgbm scikit-learn --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-read widgets after kernel restart

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
model_name_short = dbutils.widgets.get("model_name")
model_version = dbutils.widgets.get("model_version")
inference_days = int(dbutils.widgets.get("inference_days"))
mode = dbutils.widgets.get("mode")

FEATURE_TABLE = f"{catalog}.gold.feature_carbon_forecast"
FORECAST_TABLE = f"{catalog}.gold.fact_carbon_forecast"
REGISTERED_MODEL_NAME = f"{catalog}.ml.{model_name_short}"
HORIZON_H = 24

print(f"Re-loaded. Model: {REGISTERED_MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports + load model

# COMMAND ----------

from datetime import UTC, datetime, timedelta, timezone

import mlflow
import mlflow.lightgbm
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Unity Catalog as model registry
mlflow.set_registry_uri("databricks-uc")

if model_version == "latest":
    # Resolve "latest" to the highest registered version
    from mlflow.tracking import MlflowClient

    client = MlflowClient(registry_uri="databricks-uc")
    versions = client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")
    if not versions:
        raise RuntimeError(f"No registered versions found for {REGISTERED_MODEL_NAME}")
    latest_version = max(int(v.version) for v in versions)
    model_uri = f"models:/{REGISTERED_MODEL_NAME}/{latest_version}"
    print(f"Resolved 'latest' → version {latest_version}")
else:
    latest_version = int(model_version)
    model_uri = f"models:/{REGISTERED_MODEL_NAME}/{latest_version}"

print(f"Loading model: {model_uri}")
model = mlflow.lightgbm.load_model(model_uri)
print(f"✓ Model loaded. Booster has {model.num_trees()} trees, {model.num_feature()} features.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load recent features for inference

# COMMAND ----------

# Determine the inference window. We predict using features in
# [latest_feature_hour - inference_days, latest_feature_hour].
latest_feature_hour = spark.table(FEATURE_TABLE).agg(F.max("hour_utc")).collect()[0][0]
inference_start = latest_feature_hour - timedelta(days=inference_days)

print(f"Latest feature hour: {latest_feature_hour}")
print(f"Inference window:    {inference_start} → {latest_feature_hour}")

features_pdf = spark.table(FEATURE_TABLE).filter(F.col("hour_utc") >= inference_start).toPandas()
print(f"Features to score: {len(features_pdf):,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare features for LightGBM
# MAGIC
# MAGIC Same transformations as training: country_code as categorical,
# MAGIC is_weekend as int.

# COMMAND ----------

FEATURE_COLUMNS = [
    "country_code",
    "temperature_c",
    "wind_speed_kmh",
    "cloud_cover_pct",
    "solar_radiation_wm2",
    "total_generation_mw",
    "renewable_share_pct",
    "low_carbon_share_pct",
    "carbon_current",
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    "carbon_lag_1h",
    "carbon_lag_24h",
    "carbon_lag_168h",
    "carbon_rolling_24h_mean",
    "temp_rolling_24h_mean",
]
CATEGORICAL_COLUMNS = ["country_code"]

for col in CATEGORICAL_COLUMNS:
    features_pdf[col] = features_pdf[col].astype("category")
features_pdf["is_weekend"] = features_pdf["is_weekend"].astype(int)

X_infer = features_pdf[FEATURE_COLUMNS]
print(f"X_infer shape: {X_infer.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run inference

# COMMAND ----------

predictions = model.predict(X_infer)
print(f"✓ Generated {len(predictions):,} predictions")
print(f"  Min:    {predictions.min():.2f} gCO2/kWh")
print(f"  Median: {pd.Series(predictions).median():.2f} gCO2/kWh")
print(f"  Max:    {predictions.max():.2f} gCO2/kWh")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the forecast DataFrame
# MAGIC
# MAGIC One row per (country, base_hour). target_hour = base_hour + 24h.

# COMMAND ----------

now_utc = datetime.now(UTC).replace(microsecond=0)

forecast_pdf = pd.DataFrame(
    {
        "country_code": features_pdf["country_code"].astype(str),  # cast back from category
        "base_hour_utc": features_pdf["hour_utc"],
        "target_hour_utc": features_pdf["hour_utc"] + pd.Timedelta(hours=HORIZON_H),
        "horizon_h": HORIZON_H,
        "predicted_carbon_gco2_kwh": predictions,
        "carbon_current_at_base": features_pdf["carbon_current"],
        "model_version": str(latest_version),
        "model_name": REGISTERED_MODEL_NAME,
        "generated_at": now_utc,
    }
)

print(f"Forecast rows: {len(forecast_pdf):,}")
print(forecast_pdf.head(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to Spark + write to Gold
# MAGIC
# MAGIC Mode `merge` uses Delta MERGE on (country_code, base_hour_utc).
# MAGIC Mode `overwrite` replaces the whole table — useful for clean restarts.

# COMMAND ----------

FORECAST_SCHEMA = StructType(
    [
        StructField("country_code", StringType(), False),
        StructField("base_hour_utc", TimestampType(), False),
        StructField("target_hour_utc", TimestampType(), False),
        StructField("horizon_h", IntegerType(), False),
        StructField("predicted_carbon_gco2_kwh", DoubleType(), False),
        StructField("carbon_current_at_base", DoubleType(), True),
        StructField("model_version", StringType(), False),
        StructField("model_name", StringType(), False),
        StructField("generated_at", TimestampType(), False),
    ]
)

forecast_sdf = spark.createDataFrame(forecast_pdf, schema=FORECAST_SCHEMA)

# Check if target table exists
table_exists = spark.catalog.tableExists(FORECAST_TABLE)
print(f"Target table exists: {table_exists}")

if mode == "overwrite" or not table_exists:
    print(f"Writing {forecast_sdf.count():,} rows to {FORECAST_TABLE} (overwrite mode)…")
    forecast_sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        FORECAST_TABLE
    )
    print("✓ Table created/replaced")
else:
    # Stage as a temp view and MERGE on natural key
    forecast_sdf.createOrReplaceTempView("_tmp_forecast_updates")
    merge_sql = f"""
    MERGE INTO {FORECAST_TABLE} AS target
    USING _tmp_forecast_updates AS source
    ON target.country_code = source.country_code
       AND target.base_hour_utc = source.base_hour_utc
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
    print(f"MERGEing {forecast_sdf.count():,} rows into {FORECAST_TABLE}…")
    spark.sql(merge_sql)
    print("✓ MERGE complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final stats

# COMMAND ----------

final_df = spark.table(FORECAST_TABLE)
print(f"Total rows in {FORECAST_TABLE}: {final_df.count():,}")
print("\nPer-country prediction counts:")
final_df.groupBy("country_code").count().orderBy("country_code").show()

print("Recent predictions (last 5 hours per country):")
final_df.orderBy(F.desc("base_hour_utc")).limit(15).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Phase 8.D.4 — Add `get_carbon_forecast(country_code, hours_ahead=24)`
# MAGIC tool to the Streamlit agent. The tool will query this forecast table
# MAGIC and return one number: the predicted carbon intensity in 24 hours.
