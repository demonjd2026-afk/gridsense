-- Dashboard 3 — GridSense — Lakehouse Health
-- Dataset: kpi_lakehouse_summary
--
-- 1 row of headline counts across the lakehouse.
-- Powers the 3 KPI counters: total_rows | tables_tracked | layers_tracked.

WITH all_counts AS (
  SELECT 'bronze' AS layer, COUNT(*) AS rc FROM dbw_gridsense_dev.bronze.carbon_intensity
  UNION ALL SELECT 'bronze', COUNT(*) FROM dbw_gridsense_dev.bronze.entsoe
  UNION ALL SELECT 'bronze', COUNT(*) FROM dbw_gridsense_dev.bronze.open_meteo
  UNION ALL SELECT 'silver', COUNT(*) FROM dbw_gridsense_dev.silver.carbon_intensity
  UNION ALL SELECT 'silver', COUNT(*) FROM dbw_gridsense_dev.silver.country_dim
  UNION ALL SELECT 'silver', COUNT(*) FROM dbw_gridsense_dev.silver.generation
  UNION ALL SELECT 'silver', COUNT(*) FROM dbw_gridsense_dev.silver.grid_state
  UNION ALL SELECT 'silver', COUNT(*) FROM dbw_gridsense_dev.silver.weather
  UNION ALL SELECT 'gold', COUNT(*) FROM dbw_gridsense_dev.gold.dim_country
  UNION ALL SELECT 'gold', COUNT(*) FROM dbw_gridsense_dev.gold.dim_fuel_type
  UNION ALL SELECT 'gold', COUNT(*) FROM dbw_gridsense_dev.gold.dim_time
  UNION ALL SELECT 'gold', COUNT(*) FROM dbw_gridsense_dev.gold.dim_uk_region
  UNION ALL SELECT 'gold', COUNT(*) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
  UNION ALL SELECT 'gold', COUNT(*) FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly
)
SELECT
  SUM(rc) AS total_rows,
  COUNT(*) AS tables_tracked,
  COUNT(DISTINCT layer) AS layers_tracked
FROM all_counts;
