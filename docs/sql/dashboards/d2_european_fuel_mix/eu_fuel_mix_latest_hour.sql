-- Dashboard 2 — GridSense — European Fuel Mix
-- Dataset: eu_fuel_mix_latest_hour
--
-- Latest hour generation mix by (country, fuel_category). ~20 rows.
-- Powers: stacked bar chart + country×fuel ranking table.
--
-- UNIT-BUG WORKAROUND: x1000 on tons_co2_per_hour
-- The upstream column fact_generation_fuel_hourly.estimated_gco2_per_hour
-- is computed as value_mw × typical_gco2_per_kwh but should be
-- value_mw × 1000 × typical_gco2_per_kwh (MW → MWh × 1000 kWh/MWh × g/kWh).
-- We compensate with × 1000 before dividing by 1e6 to convert grams → tons.
-- Fact-table fix deferred to Phase 7.C.

WITH latest_hour AS (
  SELECT MAX(hour_utc) AS max_hour
  FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly
)
SELECT
  f.country_code,
  f.fuel_category,
  ROUND(SUM(f.value_mw), 0) AS total_mw,
  ROUND(SUM(f.estimated_gco2_per_hour) * 1000 / 1e6, 0) AS tons_co2_per_hour,
  MAX(CAST(f.is_renewable AS INT)) = 1 AS is_renewable_category,
  MAX(f.hour_utc) AS hour_utc
FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly f
CROSS JOIN latest_hour lh
WHERE f.hour_utc = lh.max_hour
GROUP BY f.country_code, f.fuel_category
ORDER BY f.country_code, total_mw DESC;
