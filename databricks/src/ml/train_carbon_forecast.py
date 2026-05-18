# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 8.D.2 — Train LightGBM for carbon forecast
# MAGIC
# MAGIC Trains a LightGBM regressor on `gold.feature_carbon_forecast` to predict
# MAGIC `target_t24h` — the carbon intensity 24 hours from now, per country.
# MAGIC
# MAGIC ## Why LightGBM (not XGBoost, not deep learning)
# MAGIC
# MAGIC LightGBM is the right tool for this problem because:
# MAGIC
# MAGIC 1. **Tabular features.** 19 numeric/categorical features per row —
# MAGIC    the kind of structured data trees excel at.
# MAGIC 2. **Native categorical support.** `country_code` passed as a string;
# MAGIC    LightGBM uses its built-in optimal-split algorithm for categoricals,
# MAGIC    no one-hot encoding needed.
# MAGIC 3. **Fast on Databricks Serverless.** Trains in <1 minute on this
# MAGIC    dataset size, vs minutes for XGBoost.
# MAGIC 4. **Interpretability.** Feature importance comes for free; matters
# MAGIC    for explaining model decisions to non-ML reviewers.
# MAGIC 5. **No GPU needed.** Pure CPU, fits Serverless Small.
# MAGIC
# MAGIC ## Why a single global model (not per-country)
# MAGIC
# MAGIC With country as a categorical feature, LightGBM learns country-specific
# MAGIC patterns inside one model. The alternative — 5 separate models —
# MAGIC quintuples the operational surface for marginal accuracy gain. Single
# MAGIC model wins for portfolio simplicity.
# MAGIC
# MAGIC ## Train/test split
# MAGIC
# MAGIC Temporal — train on data before 2026-01-01, test on 2026-01-01 onwards.
# MAGIC No random shuffle. This mimics the real-world deployment scenario:
# MAGIC train on past data, predict on future data.
# MAGIC
# MAGIC | Split | Date range | Approx rows |
# MAGIC |---|---|---|
# MAGIC | Train | 2023-05-24 → 2025-12-31 | ~115,000 |
# MAGIC | Test  | 2026-01-01 → 2026-05-15 | ~15,500 |
# MAGIC
# MAGIC ## What this notebook produces
# MAGIC
# MAGIC 1. An MLflow run with logged params, metrics, model, and feature
# MAGIC    importance plot.
# MAGIC 2. The model registered in Unity Catalog Model Registry as
# MAGIC    `dbw_gridsense_dev.ml.carbon_forecast_lgb`, version N.
# MAGIC 3. Per-country evaluation metrics printed inline for transparency.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "dbw_gridsense_dev")
dbutils.widgets.text("test_split_date", "2026-01-01")
dbutils.widgets.text("model_name", "carbon_forecast_lgb")
dbutils.widgets.dropdown("register_model", "true", ["true", "false"])

catalog = dbutils.widgets.get("catalog")
test_split_date = dbutils.widgets.get("test_split_date")
model_name_short = dbutils.widgets.get("model_name")
register_model = dbutils.widgets.get("register_model") == "true"

FEATURE_TABLE = f"{catalog}.gold.feature_carbon_forecast"
REGISTERED_MODEL_NAME = f"{catalog}.ml.{model_name_short}"
EXPERIMENT_NAME = "/Shared/gridsense_carbon_forecast"

print(f"Feature table:    {FEATURE_TABLE}")
print(f"Test split date:  {test_split_date}")
print(f"Model name:       {REGISTERED_MODEL_NAME}")
print(f"Register model:   {register_model}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install LightGBM
# MAGIC
# MAGIC LightGBM is not in the default Databricks runtime; install + kernel
# MAGIC restart at the top of the notebook (same pattern as the ENTSO-E
# MAGIC backfill notebook which needed `xmltodict`).

# COMMAND ----------

# MAGIC %pip install lightgbm scikit-learn --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-read widgets after kernel restart
# MAGIC
# MAGIC `%pip install` triggers a Python kernel restart in Serverless, which
# MAGIC wipes Python variables. Re-read widgets here so downstream cells
# MAGIC have the values they need.

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
test_split_date = dbutils.widgets.get("test_split_date")
model_name_short = dbutils.widgets.get("model_name")
register_model = dbutils.widgets.get("register_model") == "true"

FEATURE_TABLE = f"{catalog}.gold.feature_carbon_forecast"
REGISTERED_MODEL_NAME = f"{catalog}.ml.{model_name_short}"
EXPERIMENT_NAME = "/Shared/gridsense_carbon_forecast"

print(f"Re-loaded after kernel restart. Feature table: {FEATURE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports + MLflow setup

# COMMAND ----------

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Set the MLflow experiment. Creates if it doesn't exist.
mlflow.set_experiment(EXPERIMENT_NAME)

# Use Unity Catalog as model registry (rather than Workspace registry).
mlflow.set_registry_uri("databricks-uc")

print(f"MLflow experiment: {EXPERIMENT_NAME}")
print("MLflow registry:   databricks-uc (Unity Catalog)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load features and split temporally

# COMMAND ----------

features_pdf = spark.table(FEATURE_TABLE).toPandas()
print(f"Loaded {len(features_pdf):,} feature rows")
print(f"Date range: {features_pdf['hour_utc'].min()} → {features_pdf['hour_utc'].max()}")

# Temporal split — strictly chronological, no shuffle
split_ts = pd.Timestamp(test_split_date)
train_pdf = features_pdf[features_pdf["hour_utc"] < split_ts].copy()
test_pdf = features_pdf[features_pdf["hour_utc"] >= split_ts].copy()

print(
    f"\nTrain rows: {len(train_pdf):,} ({train_pdf['hour_utc'].min()} → {train_pdf['hour_utc'].max()})"
)
print(
    f"Test rows:  {len(test_pdf):,} ({test_pdf['hour_utc'].min()} → {test_pdf['hour_utc'].max()})"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature/target selection
# MAGIC
# MAGIC Pull out the 19 features and the target. `country_code` stays as a
# MAGIC pandas categorical so LightGBM treats it natively.

# COMMAND ----------

FEATURE_COLUMNS = [
    # Categorical
    "country_code",
    # Current grid state
    "temperature_c",
    "wind_speed_kmh",
    "cloud_cover_pct",
    "solar_radiation_wm2",
    "total_generation_mw",
    "renewable_share_pct",
    "low_carbon_share_pct",
    "carbon_current",
    # Calendar
    "hour_of_day",
    "day_of_week",
    "month",
    "is_weekend",
    # Lag
    "carbon_lag_1h",
    "carbon_lag_24h",
    "carbon_lag_168h",
    # Rolling
    "carbon_rolling_24h_mean",
    "temp_rolling_24h_mean",
]
TARGET_COLUMN = "target_t24h"
CATEGORICAL_COLUMNS = ["country_code"]

# Convert categorical column to pandas category dtype.
for col in CATEGORICAL_COLUMNS:
    train_pdf[col] = train_pdf[col].astype("category")
    test_pdf[col] = test_pdf[col].astype("category")

# Also convert is_weekend to int (LightGBM handles bool but explicit int is safer)
train_pdf["is_weekend"] = train_pdf["is_weekend"].astype(int)
test_pdf["is_weekend"] = test_pdf["is_weekend"].astype(int)

X_train = train_pdf[FEATURE_COLUMNS]
y_train = train_pdf[TARGET_COLUMN]
X_test = test_pdf[FEATURE_COLUMNS]
y_test = test_pdf[TARGET_COLUMN]

print(f"X_train shape: {X_train.shape}")
print(f"X_test shape:  {X_test.shape}")
print(f"Features:      {FEATURE_COLUMNS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train LightGBM with MLflow tracking
# MAGIC
# MAGIC Reasonable starting hyperparams. Not exhaustively tuned — the goal
# MAGIC here is a working baseline that demonstrates the end-to-end pipeline
# MAGIC and produces useful predictions. Hyperparameter tuning would be a
# MAGIC follow-up (Phase 8.E if pursued).

# COMMAND ----------

LGB_PARAMS = {
    "objective": "regression",
    "metric": "mae",
    "learning_rate": 0.05,
    "num_leaves": 63,
    "max_depth": -1,
    "min_data_in_leaf": 50,
    "feature_fraction": 0.9,
    "bagging_fraction": 0.9,
    "bagging_freq": 5,
    "verbose": -1,
    "seed": 42,
}
NUM_ROUNDS = 500
EARLY_STOPPING_ROUNDS = 25

with mlflow.start_run(run_name="lgb_carbon_forecast_v1") as run:
    print(f"MLflow run_id: {run.info.run_id}")

    # Log basic info
    mlflow.log_params(LGB_PARAMS)
    mlflow.log_param("num_rounds", NUM_ROUNDS)
    mlflow.log_param("early_stopping_rounds", EARLY_STOPPING_ROUNDS)
    mlflow.log_param("feature_columns", str(FEATURE_COLUMNS))
    mlflow.log_param("train_rows", len(X_train))
    mlflow.log_param("test_rows", len(X_test))
    mlflow.log_param("test_split_date", test_split_date)

    # LightGBM Datasets
    train_set = lgb.Dataset(X_train, label=y_train, categorical_feature=CATEGORICAL_COLUMNS)
    test_set = lgb.Dataset(
        X_test, label=y_test, categorical_feature=CATEGORICAL_COLUMNS, reference=train_set
    )

    # Train with early stopping on the test set (used as a validation set here)
    model = lgb.train(
        LGB_PARAMS,
        train_set,
        num_boost_round=NUM_ROUNDS,
        valid_sets=[train_set, test_set],
        valid_names=["train", "test"],
        callbacks=[
            lgb.early_stopping(EARLY_STOPPING_ROUNDS),
            lgb.log_evaluation(period=50),
        ],
    )

    # Predictions on the test set
    y_pred = model.predict(X_test)

    # Metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    r2 = r2_score(y_test, y_pred)
    # MAPE — handle zero-division by clipping target to a small positive floor
    mape = float(np.mean(np.abs((y_test - y_pred) / np.clip(y_test, 1, None))) * 100)

    print("\n=== Test set performance ===")
    print(f"MAE:  {mae:.2f} gCO2/kWh")
    print(f"RMSE: {rmse:.2f} gCO2/kWh")
    print(f"R²:   {r2:.3f}")
    print(f"MAPE: {mape:.1f}%")

    mlflow.log_metric("test_mae", mae)
    mlflow.log_metric("test_rmse", rmse)
    mlflow.log_metric("test_r2", r2)
    mlflow.log_metric("test_mape", mape)

    # Per-country metrics — important for transparency
    print("\n=== Per-country performance ===")
    per_country = test_pdf.copy()
    per_country["predicted"] = y_pred
    for country in sorted(per_country["country_code"].dropna().unique()):
        sub = per_country[per_country["country_code"] == country]
        country_mae = mean_absolute_error(sub[TARGET_COLUMN], sub["predicted"])
        country_r2 = r2_score(sub[TARGET_COLUMN], sub["predicted"])
        country_avg_actual = sub[TARGET_COLUMN].mean()
        rel_err = (country_mae / country_avg_actual) * 100
        print(
            f"  {country}: MAE={country_mae:6.2f} gCO2/kWh "
            f"(rel {rel_err:5.1f}%) | R²={country_r2:.3f} | "
            f"avg target={country_avg_actual:6.1f}"
        )
        mlflow.log_metric(f"test_mae_{country}", country_mae)
        mlflow.log_metric(f"test_r2_{country}", country_r2)

    # Feature importance — log as artifact for the MLflow UI
    importance_df = pd.DataFrame(
        {
            "feature": model.feature_name(),
            "importance": model.feature_importance(importance_type="gain"),
        }
    ).sort_values("importance", ascending=False)
    print("\n=== Top 10 features by gain ===")
    print(importance_df.head(10).to_string(index=False))

    importance_csv = "/tmp/feature_importance.csv"
    importance_df.to_csv(importance_csv, index=False)
    mlflow.log_artifact(importance_csv)

    # Log the model itself + optionally register to Unity Catalog
    if register_model:
        print(f"\nLogging + registering model to {REGISTERED_MODEL_NAME}…")
        # Signature is inferred from a small input sample for downstream tooling
        signature = mlflow.models.infer_signature(X_train.head(5), model.predict(X_train.head(5)))
        mlflow.lightgbm.log_model(
            lgb_model=model,
            artifact_path="model",
            registered_model_name=REGISTERED_MODEL_NAME,
            signature=signature,
            input_example=X_train.head(5),
        )
        print(f"✓ Registered as {REGISTERED_MODEL_NAME}")
    else:
        print("\nLogging model only (skipping registration)…")
        signature = mlflow.models.infer_signature(X_train.head(5), model.predict(X_train.head(5)))
        mlflow.lightgbm.log_model(
            lgb_model=model,
            artifact_path="model",
            signature=signature,
            input_example=X_train.head(5),
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The trained model is now:
# MAGIC 1. Logged to MLflow experiment `/Shared/gridsense_carbon_forecast`
# MAGIC 2. Registered (if `register_model=true`) in Unity Catalog as
# MAGIC    `dbw_gridsense_dev.ml.carbon_forecast_lgb`
# MAGIC
# MAGIC ## Next steps
# MAGIC
# MAGIC Phase 8.D.3 — Inference notebook that:
# MAGIC   - Loads the registered model
# MAGIC   - Generates predictions for the latest available features
# MAGIC   - Writes results to `gold.fact_carbon_forecast`
# MAGIC
# MAGIC Phase 8.D.4 — Streamlit agent tool that queries the forecast fact.
