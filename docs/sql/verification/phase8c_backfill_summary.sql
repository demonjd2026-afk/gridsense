-- ============================================================================
-- Phase 8.C verification — Open-Meteo historical weather backfill
-- ----------------------------------------------------------------------------
-- Run AFTER:
--   1. backfill_open_meteo job completes
--   2. silver_open_meteo re-runs to MERGE into silver.weather
--   3. silver_grid_state re-runs (3-way join now has weather history)
--   4. gold_fact_grid_hourly re-runs (now unblocked for 3-year coverage)
--
-- Expected magnitudes (3yr × 6 cities × 24 hours):
--   - Per city per year: ~8,760 rows
--   - 6 cities × 3 years: ~157,680 rows
-- ============================================================================

-- 1. Bronze: live vs backfill split
SELECT
  get_json_object(envelope_json, '$.source') AS source,
  COUNT(*)                                   AS rows,
  COUNT(DISTINCT kafka_key)                  AS distinct_cities,
  MIN(event_date)                            AS earliest_date,
  MAX(event_date)                            AS latest_date
FROM dbw_gridsense_dev.bronze.open_meteo
GROUP BY source
ORDER BY source;

-- 2. Silver: post-MERGE total
SELECT
  COUNT(*)                                       AS row_count,
  COUNT(DISTINCT city, time_utc)                 AS distinct_natural_keys,
  COUNT(DISTINCT city)                           AS distinct_cities,
  MIN(time_utc)                                  AS earliest,
  MAX(time_utc)                                  AS latest,
  DATEDIFF(DAY, MIN(time_utc), MAX(time_utc))    AS span_days
FROM dbw_gridsense_dev.silver.weather;

-- 3. Silver: per-city, per-year coverage
SELECT
  city,
  YEAR(time_utc)                AS year,
  COUNT(*)                      AS hours,
  ROUND(AVG(temperature_c), 1)  AS avg_temp_c,
  ROUND(MAX(temperature_c), 1)  AS peak_temp_c,
  ROUND(MIN(temperature_c), 1)  AS min_temp_c
FROM dbw_gridsense_dev.silver.weather
GROUP BY city, YEAR(time_utc)
ORDER BY city, year;

-- 4. Gold fact_grid_hourly: should now span 3 years (unblocked by 8.C)
SELECT
  COUNT(*)                                       AS row_count,
  MIN(hour_utc)                                  AS earliest,
  MAX(hour_utc)                                  AS latest,
  COUNT(DISTINCT country_key)                    AS countries
FROM dbw_gridsense_dev.gold.fact_grid_hourly;
