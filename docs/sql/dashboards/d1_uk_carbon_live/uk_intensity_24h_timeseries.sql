-- Dashboard 1 — GridSense — UK Carbon Live
-- Dataset: uk_intensity_24h_timeseries
--
-- Last 24 hours of intensity for each UK region (~864 rows: 18 regions x 48 periods).
-- Powers: 24h time-series line chart + region multi-select filter (shared dataset cascade).

SELECT
  region_name,
  region_type,
  period_start,
  intensity_forecast AS gco2_per_kwh,
  intensity_index
FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
WHERE period_start >= DATEADD(
  hour, -24,
  (SELECT MAX(period_start) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min)
)
ORDER BY region_name, period_start;
