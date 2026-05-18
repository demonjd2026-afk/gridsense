-- ============================================================================
-- Phase 8.D.3 verification — gold.fact_carbon_forecast
-- ----------------------------------------------------------------------------
-- Run AFTER ml_infer_carbon_forecast job completes.
--
-- Expected (default inference_days=7):
--   - ~840 rows (5 countries × 168 hours = 840)
--   - All predictions look like physically-plausible gCO2/kWh values
--   - base_hour_utc spans the last 7 days
--   - target_hour_utc = base_hour_utc + 24h for every row
-- ============================================================================

-- 1. Schema sanity
DESCRIBE dbw_gridsense_dev.gold.fact_carbon_forecast;

-- 2. Row count, range, country coverage
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT country_code) AS countries,
  MIN(base_hour_utc) AS earliest_base,
  MAX(base_hour_utc) AS latest_base,
  MIN(target_hour_utc) AS earliest_target,
  MAX(target_hour_utc) AS latest_target,
  COUNT(DISTINCT model_version) AS distinct_model_versions
FROM dbw_gridsense_dev.gold.fact_carbon_forecast;

-- 3. Per-country prediction distribution sanity
SELECT
  country_code,
  COUNT(*) AS predictions,
  ROUND(MIN(predicted_carbon_gco2_kwh)) AS min_pred,
  ROUND(PERCENTILE_APPROX(predicted_carbon_gco2_kwh, 0.5)) AS median_pred,
  ROUND(MAX(predicted_carbon_gco2_kwh)) AS max_pred,
  ROUND(STDDEV(predicted_carbon_gco2_kwh)) AS stddev_pred
FROM dbw_gridsense_dev.gold.fact_carbon_forecast
GROUP BY country_code
ORDER BY country_code;

-- 4. Forecast vs. current — does the model predict change?
-- For each row: how different is the prediction from the carbon AT base time?
SELECT
  country_code,
  ROUND(AVG(predicted_carbon_gco2_kwh - carbon_current_at_base), 1) AS avg_delta,
  ROUND(AVG(ABS(predicted_carbon_gco2_kwh - carbon_current_at_base)), 1) AS avg_abs_delta,
  ROUND(MAX(ABS(predicted_carbon_gco2_kwh - carbon_current_at_base)), 1) AS max_abs_delta
FROM dbw_gridsense_dev.gold.fact_carbon_forecast
GROUP BY country_code
ORDER BY country_code;

-- 5. Most recent prediction per country (what the agent will surface "right now")
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY base_hour_utc DESC) AS rn
  FROM dbw_gridsense_dev.gold.fact_carbon_forecast
)
SELECT
  country_code,
  base_hour_utc,
  target_hour_utc,
  ROUND(carbon_current_at_base, 1) AS current_gco2_kwh,
  ROUND(predicted_carbon_gco2_kwh, 1) AS predicted_gco2_kwh_t24h,
  model_version
FROM ranked
WHERE rn = 1
ORDER BY country_code;
