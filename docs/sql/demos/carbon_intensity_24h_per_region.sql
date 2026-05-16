-- ============================================================================
-- DEMO / Phase 10 prep — 24h carbon intensity per UK region
-- ----------------------------------------------------------------------------
-- Time-series view: every 30-min interval for the last 24 hours for every
-- UK region. This is the dataset that will power the main Power BI
-- line-chart visual in Phase 10 — region picker on top, time-series
-- below, intensity_index colour-coded markers.
--
-- Filtered to actual UK regions (excludes the GB national rollup so the
-- chart isn't dominated by one line). Power BI can re-enable GB as a
-- comparison series via a slicer.
-- ============================================================================

SELECT
  region_name,
  region_type,
  period_start,
  intensity_forecast AS gco2_per_kwh,
  intensity_index
FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
WHERE period_start >= (SELECT DATEADD(hour, -24, MAX(period_start)) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min)
  AND region_type = 'DNO'
ORDER BY region_name, period_start;
