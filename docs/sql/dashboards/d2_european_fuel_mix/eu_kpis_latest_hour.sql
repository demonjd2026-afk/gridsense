-- Dashboard 2 — GridSense — European Fuel Mix
-- Dataset: eu_kpis_latest_hour
--
-- 1 row of headline metrics across reporting EU countries at the latest hour.
-- Powers the 3 KPI counters (Total MW | Lifecycle CO₂ tons/hour | Renewable %).
--
-- estimated_gco2_per_hour is in grams CO₂/hour after the Phase 7.C unit fix
-- (see databricks/src/gold/fact_generation_fuel_hourly.py).

WITH latest_hour AS (
  SELECT MAX(hour_utc) AS max_hour
  FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly
)
SELECT
  ROUND(SUM(f.value_mw), 0) AS total_mw,
  ROUND(SUM(f.estimated_gco2_per_hour) / 1e6, 0) AS total_tons_co2_per_hour,
  ROUND(
    100.0 * SUM(CASE WHEN f.is_renewable THEN f.value_mw ELSE 0 END) / SUM(f.value_mw),
    1
  ) AS renewable_pct,
  ROUND(
    SUM(f.value_mw * f.typical_gco2_per_kwh) / SUM(f.value_mw),
    0
  ) AS eu_avg_gco2_per_kwh,
  COUNT(DISTINCT f.country_code) AS countries_reporting
FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly f
CROSS JOIN latest_hour lh
WHERE f.hour_utc = lh.max_hour;
