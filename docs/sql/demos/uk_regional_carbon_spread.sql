-- ============================================================================
-- DEMO — UK regional carbon intensity spread
-- ----------------------------------------------------------------------------
-- THE headline query for GridSense. Shows the dirtiest vs cleanest UK
-- regions at the latest available 30-min interval. The spread between
-- the two extremes is the entire pitch for carbon-aware workload placement.
--
-- Observed example (2026-05-16 04:00 UTC):
--   - South West / South Wales: 382 gCO2/kWh (very high)
--   - Scotland regions: 0 gCO2/kWh (very low)
-- A workload running in Scotland emits ZERO grams of CO2 per kWh at this
-- moment. The same workload in South West emits 382 g/kWh. Same country,
-- same minute.
-- ============================================================================

SELECT
  region_name,
  region_type,
  period_start,
  intensity_forecast AS gco2_per_kwh,
  intensity_index,
  source_type,
  CASE WHEN intensity_actual IS NOT NULL
       THEN intensity_actual - intensity_forecast
       ELSE NULL
  END AS actual_minus_forecast
FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
WHERE period_start = (SELECT MAX(period_start) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min)
ORDER BY intensity_forecast DESC;
