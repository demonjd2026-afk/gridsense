-- Dashboard 1 — GridSense — UK Carbon Live
-- Dataset: uk_kpis_latest
--
-- Single-row aggregate for the 4 headline KPI counters.
-- Each counter binds one column with aggregation=None (already aggregated).

SELECT
  MAX(period_start) AS latest_period,
  MAX(intensity_forecast) AS dirtiest_gco2_per_kwh,
  MIN(intensity_forecast) AS cleanest_gco2_per_kwh,
  AVG(intensity_forecast) AS avg_gco2_per_kwh,
  (
    SELECT intensity_forecast
    FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
    WHERE period_start = (
      SELECT MAX(period_start) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
    )
      AND region_id = 18
  ) AS gb_national_gco2_per_kwh
FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
WHERE period_start = (
  SELECT MAX(period_start) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
);
