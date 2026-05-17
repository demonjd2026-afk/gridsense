-- Dashboard 3 — GridSense — Lakehouse Health
-- Dataset: layer_row_counts
--
-- One row per table across the medallion layers (14 rows).
-- bronze: 3 (one per source API)
-- silver: 5 (carbon_intensity, country_dim, generation, grid_state, weather)
-- gold:   6 (4 dims + 2 facts)

SELECT 'bronze' AS layer, 'carbon_intensity' AS table_name, COUNT(*) AS row_count FROM dbw_gridsense_dev.bronze.carbon_intensity
UNION ALL SELECT 'bronze', 'entsoe', COUNT(*) FROM dbw_gridsense_dev.bronze.entsoe
UNION ALL SELECT 'bronze', 'open_meteo', COUNT(*) FROM dbw_gridsense_dev.bronze.open_meteo
UNION ALL SELECT 'silver', 'carbon_intensity', COUNT(*) FROM dbw_gridsense_dev.silver.carbon_intensity
UNION ALL SELECT 'silver', 'country_dim', COUNT(*) FROM dbw_gridsense_dev.silver.country_dim
UNION ALL SELECT 'silver', 'generation', COUNT(*) FROM dbw_gridsense_dev.silver.generation
UNION ALL SELECT 'silver', 'grid_state', COUNT(*) FROM dbw_gridsense_dev.silver.grid_state
UNION ALL SELECT 'silver', 'weather', COUNT(*) FROM dbw_gridsense_dev.silver.weather
UNION ALL SELECT 'gold', 'dim_country', COUNT(*) FROM dbw_gridsense_dev.gold.dim_country
UNION ALL SELECT 'gold', 'dim_fuel_type', COUNT(*) FROM dbw_gridsense_dev.gold.dim_fuel_type
UNION ALL SELECT 'gold', 'dim_time', COUNT(*) FROM dbw_gridsense_dev.gold.dim_time
UNION ALL SELECT 'gold', 'dim_uk_region', COUNT(*) FROM dbw_gridsense_dev.gold.dim_uk_region
UNION ALL SELECT 'gold', 'fact_carbon_intensity_30min', COUNT(*) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
UNION ALL SELECT 'gold', 'fact_generation_fuel_hourly', COUNT(*) FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly
ORDER BY
  CASE layer WHEN 'bronze' THEN 1 WHEN 'silver' THEN 2 WHEN 'gold' THEN 3 END,
  table_name;
