-- ============================================================================
-- Phase 8.D.2 verification — LightGBM training run
-- ----------------------------------------------------------------------------
-- After ml_train_carbon_forecast completes, inspect the MLflow run in the
-- workspace UI:
--   Workspace → /Shared/gridsense_carbon_forecast → most recent run
--
-- The SQL below queries the system tables that track Unity Catalog model
-- registry state, so you can verify the registration landed.
-- ============================================================================

-- 1. Is the model registered in Unity Catalog?
SHOW MODELS IN dbw_gridsense_dev.ml;

-- 2. What versions exist?
-- Replace `carbon_forecast_lgb` if a different model_name parameter was used.
SHOW MODEL VERSIONS dbw_gridsense_dev.ml.carbon_forecast_lgb;

-- 3. Feature table sanity (should be ~130K rows from Phase 8.D.1)
SELECT
  COUNT(*) AS rows,
  COUNT(DISTINCT country_code) AS countries,
  MIN(hour_utc) AS earliest,
  MAX(hour_utc) AS latest
FROM dbw_gridsense_dev.gold.feature_carbon_forecast;

-- 4. After Phase 8.D.3 ships, gold.fact_carbon_forecast will appear here.
-- For now, this should return nothing (or error if table doesn't exist yet).
-- SELECT COUNT(*) FROM dbw_gridsense_dev.gold.fact_carbon_forecast;
