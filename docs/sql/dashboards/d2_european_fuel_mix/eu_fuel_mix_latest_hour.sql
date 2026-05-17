-- Dashboard 2 — GridSense — European Fuel Mix
-- Dataset: eu_fuel_mix_latest_hour
--
-- Latest hour generation mix by (country, fuel_category). ~20 rows.
-- Powers: stacked bar chart + country×fuel ranking table.
--
-- Note: estimated_gco2_per_hour is in grams CO₂/hour (post Phase 7.C unit fix).
-- Divide by 1e6 to convert grams → tons.

WITH latest_hour AS (
  SELECT MAX(hour_utc) AS max_hour
  FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly
)
SELECT
  f.country_code,
  f.fuel_category,
  ROUND(SUM(f.value_mw), 0) AS total_mw,
  ROUND(SUM(f.estimated_gco2_per_hour) / 1e6, 0) AS tons_co2_per_hour,
  MAX(CAST(f.is_renewable AS INT)) = 1 AS is_renewable_category,
  MAX(f.hour_utc) AS hour_utc
FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly f
CROSS JOIN latest_hour lh
WHERE f.hour_utc = lh.max_hour
GROUP BY f.country_code, f.fuel_category
ORDER BY f.country_code, total_mw DESC;
