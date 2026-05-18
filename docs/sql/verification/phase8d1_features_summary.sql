-- ============================================================================
-- Phase 8.D.1 verification — gold.feature_carbon_forecast
-- ----------------------------------------------------------------------------
-- Run AFTER ml_features_carbon_forecast job completes.
--
-- Expected (from ~131K hours of fact_grid_hourly across 5 countries):
--   - Total feature rows: ~125,000 (after dropping NULL boundary rows
--     for first week + last day per country)
--   - Per country: ~25,000 rows
--   - Date span: 2023-05-24 → 2026-05-15 (skipping first week and last day)
--   - All numeric features have realistic distributions
-- ============================================================================

-- 1. Schema sanity
DESCRIBE dbw_gridsense_dev.gold.feature_carbon_forecast;

-- 2. Row counts overall + per country
SELECT
  COUNT(*) AS row_count,
  COUNT(DISTINCT country_code) AS distinct_countries,
  MIN(hour_utc) AS earliest,
  MAX(hour_utc) AS latest,
  DATEDIFF(DAY, MIN(hour_utc), MAX(hour_utc)) AS span_days
FROM dbw_gridsense_dev.gold.feature_carbon_forecast;

-- 3. Per-country, per-year coverage
SELECT
  country_code,
  YEAR(hour_utc) AS year,
  COUNT(*) AS rows,
  ROUND(AVG(carbon_current)) AS avg_carbon,
  ROUND(AVG(target_t24h)) AS avg_target,
  ROUND(AVG(temperature_c), 1) AS avg_temp_c
FROM dbw_gridsense_dev.gold.feature_carbon_forecast
GROUP BY country_code, YEAR(hour_utc)
ORDER BY country_code, year;

-- 4. Feature distribution sanity — carbon intensity ranges by country
-- (FR nuclear-dominant should be low; DE coal-mixed should be higher)
SELECT
  country_code,
  ROUND(MIN(carbon_current)) AS min_gco2_kwh,
  ROUND(PERCENTILE_APPROX(carbon_current, 0.5)) AS median_gco2_kwh,
  ROUND(MAX(carbon_current)) AS max_gco2_kwh,
  ROUND(STDDEV(carbon_current)) AS stddev
FROM dbw_gridsense_dev.gold.feature_carbon_forecast
GROUP BY country_code
ORDER BY country_code;

-- 5. Correlation sanity — carbon vs renewable share
-- (Higher renewable share should correlate with lower carbon intensity)
SELECT
  country_code,
  ROUND(CORR(renewable_share_pct, carbon_current), 3) AS corr_renewable_carbon,
  ROUND(CORR(low_carbon_share_pct, carbon_current), 3) AS corr_low_carbon_carbon,
  ROUND(CORR(carbon_lag_24h, carbon_current), 3) AS corr_lag24_current,
  ROUND(CORR(carbon_current, target_t24h), 3) AS corr_current_t24h
FROM dbw_gridsense_dev.gold.feature_carbon_forecast
GROUP BY country_code
ORDER BY country_code;

-- 6. Null check — should be zero
SELECT
  SUM(CASE WHEN target_t24h IS NULL THEN 1 ELSE 0 END) AS null_target,
  SUM(CASE WHEN carbon_lag_168h IS NULL THEN 1 ELSE 0 END) AS null_lag_168h,
  SUM(CASE WHEN carbon_rolling_24h_mean IS NULL THEN 1 ELSE 0 END) AS null_rolling,
  SUM(CASE WHEN temperature_c IS NULL THEN 1 ELSE 0 END) AS null_temp
FROM dbw_gridsense_dev.gold.feature_carbon_forecast;
