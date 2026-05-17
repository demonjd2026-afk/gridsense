-- Dashboard 3 — GridSense — Lakehouse Health
-- Dataset: freshness_per_table
--
-- Most-recent BUSINESS timestamp per table + age in minutes + status bucket.
-- Business timestamp = the hour/period the data represents, not when it was
-- ingested. A row with fresh ingested_at but 24h-old period_start is not
-- fresh from a consumer's perspective.
--
-- Thresholds: fresh <60 min, recent <240, stale <1440, very stale >=1440.

WITH freshness AS (
  SELECT 'silver' AS layer, 'carbon_intensity' AS table_name,
         MAX(period_start) AS latest_ts
  FROM dbw_gridsense_dev.silver.carbon_intensity
  UNION ALL
  SELECT 'silver', 'generation', MAX(period_start)
  FROM dbw_gridsense_dev.silver.generation
  UNION ALL
  SELECT 'silver', 'weather', MAX(time_utc)
  FROM dbw_gridsense_dev.silver.weather
  UNION ALL
  SELECT 'silver', 'grid_state', MAX(hour_utc)
  FROM dbw_gridsense_dev.silver.grid_state
  UNION ALL
  SELECT 'gold', 'fact_carbon_intensity_30min', MAX(period_start)
  FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
  UNION ALL
  SELECT 'gold', 'fact_generation_fuel_hourly', MAX(hour_utc)
  FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly
)
SELECT
  layer,
  table_name,
  latest_ts,
  ROUND(
    (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(latest_ts)) / 60.0,
    1
  ) AS age_minutes,
  CASE
    WHEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(latest_ts)) / 60.0 < 60   THEN 'fresh'
    WHEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(latest_ts)) / 60.0 < 240  THEN 'recent'
    WHEN (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(latest_ts)) / 60.0 < 1440 THEN 'stale'
    ELSE 'very stale'
  END AS freshness_status
FROM freshness
ORDER BY age_minutes;
