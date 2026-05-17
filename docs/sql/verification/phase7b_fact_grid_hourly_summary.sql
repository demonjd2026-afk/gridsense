-- ============================================================================
-- Phase 7.B verification — fact_grid_hourly
-- ----------------------------------------------------------------------------
-- Purpose: Confirm row count, grain (one row per country × hour), null
-- counts on FKs, and measure range sanity.
-- When to use: After every gold_fact_grid_hourly run.
-- Expected (post first build, ~70h of data × 5-6 countries):
--   - row_count in the 300-500 range
--   - distinct_natural_keys == row_count (clean grain, no dups)
--   - avg_renewable_pct between 30-50 (Europe average, varies by hour)
--   - avg_low_carbon_pct >= avg_renewable_pct (nuclear adds, never subtracts)
--   - rows_with_uk_carbon > 0 (UK measured intensity surfaces on GB rows)
-- ============================================================================

SELECT
  COUNT(*)                                                          AS row_count,
  COUNT(DISTINCT country_code, hour_utc)                            AS distinct_natural_keys,
  COUNT(DISTINCT country_code)                                      AS countries,
  COUNT(DISTINCT hour_utc)                                          AS hours,
  MIN(hour_utc)                                                     AS earliest,
  MAX(hour_utc)                                                     AS latest,
  ROUND(AVG(renewable_share_pct), 1)                                AS avg_renewable_pct,
  ROUND(AVG(low_carbon_share_pct), 1)                               AS avg_low_carbon_pct,
  ROUND(AVG(estimated_lifecycle_gco2_per_kwh), 0)                   AS avg_gco2_per_kwh,
  SUM(CASE WHEN uk_carbon_intensity_forecast IS NOT NULL THEN 1
           ELSE 0 END)                                              AS rows_with_uk_carbon
FROM dbw_gridsense_dev.gold.fact_grid_hourly;

-- Per-country latest-hour snapshot — should show FR very low (nuclear-heavy),
-- DE/PL high (fossil-heavy if available), countries in between varying by hour.
SELECT
  country_code,
  hour_utc,
  ROUND(total_generation_mw, 0)            AS total_mw,
  ROUND(renewable_generation_mw, 0)        AS renewable_mw,
  ROUND(low_carbon_generation_mw, 0)       AS low_carbon_mw,
  renewable_share_pct,
  low_carbon_share_pct,
  estimated_lifecycle_gco2_per_kwh         AS gco2_per_kwh,
  ROUND(estimated_lifecycle_gco2_per_hour / 1e6, 0)
                                            AS tons_co2_per_hour,
  uk_carbon_intensity_forecast
FROM dbw_gridsense_dev.gold.fact_grid_hourly
WHERE hour_utc = (SELECT MAX(hour_utc) FROM dbw_gridsense_dev.gold.fact_grid_hourly)
ORDER BY estimated_lifecycle_gco2_per_kwh DESC;
