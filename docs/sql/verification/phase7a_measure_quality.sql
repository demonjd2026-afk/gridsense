-- ============================================================================
-- Phase 7.A — fact_generation_fuel_hourly measure quality
-- ----------------------------------------------------------------------------
-- Purpose: Confirm no nulls on FKs or measure, MW range is plausible.
-- When to use: After ETL changes affecting silver.generation or the Gold fact.
-- Expected:
--   - All null counts = 0
--   - min_mw >= 0 (negative only for pumped-hydro consumption if modeled)
--   - max_mw in the low tens of thousands (single fuel in a big country)
-- ============================================================================

SELECT
  SUM(CASE WHEN country_key IS NULL THEN 1 ELSE 0 END) AS null_country_fk,
  SUM(CASE WHEN fuel_key    IS NULL THEN 1 ELSE 0 END) AS null_fuel_fk,
  SUM(CASE WHEN time_key    IS NULL THEN 1 ELSE 0 END) AS null_time_fk,
  SUM(CASE WHEN value_mw    IS NULL THEN 1 ELSE 0 END) AS null_value_mw,
  ROUND(MIN(value_mw), 2) AS min_mw,
  ROUND(MAX(value_mw), 2) AS max_mw,
  ROUND(AVG(value_mw), 2) AS avg_mw
FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly;
