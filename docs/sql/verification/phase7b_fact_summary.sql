-- ============================================================================
-- Phase 7.B — fact_carbon_intensity_30min summary
-- ----------------------------------------------------------------------------
-- Purpose: Confirm grain is one row per (region_id, period_start), 18 regions
-- present, forecast always populated, source_type discriminator working.
-- When to use: After gold_fact_carbon_intensity_30min runs.
-- Expected:
--   - row_count == distinct_natural_keys (clean grain, no dups)
--   - regions = 18
--   - forecast_rows = row_count, actual_rows = 0 initially (rises when
--     the UK API backfills actuals, ~48h after period close)
--   - All null counts = 0
-- ============================================================================

SELECT
  COUNT(*)                                                       AS row_count,
  COUNT(DISTINCT region_id, period_start)                        AS distinct_natural_keys,
  COUNT(DISTINCT region_id)                                      AS regions,
  MIN(period_start)                                              AS earliest,
  MAX(period_start)                                              AS latest,
  SUM(CASE WHEN source_type = 'forecast' THEN 1 ELSE 0 END)      AS forecast_rows,
  SUM(CASE WHEN source_type = 'actual'   THEN 1 ELSE 0 END)      AS actual_rows,
  SUM(CASE WHEN intensity_forecast IS NULL THEN 1 ELSE 0 END)    AS null_forecast,
  SUM(CASE WHEN time_key IS NULL THEN 1 ELSE 0 END)              AS null_time_key
FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min;
