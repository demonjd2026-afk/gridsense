-- Dashboard 2 — GridSense — European Fuel Mix
-- Dataset: eu_co2_24h_per_country
--
-- Total lifecycle CO₂ per country per hour for the last 24 hours.
-- ~119 rows (6 countries × ~20 hours; gaps where TSOs have not yet published).
-- Powers the headline line chart (FR flat at ~1.1k vs DE swing 9k–16k).
--
-- x1000 unit-bug workaround documented in eu_fuel_mix_latest_hour.sql.

SELECT
  f.country_code,
  f.hour_utc,
  ROUND(SUM(f.estimated_gco2_per_hour) * 1000 / 1e6, 0) AS tons_co2_per_hour,
  ROUND(SUM(f.value_mw), 0) AS total_mw
FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly f
WHERE f.hour_utc >= DATEADD(
  hour, -24,
  (SELECT MAX(hour_utc) FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly)
)
GROUP BY f.country_code, f.hour_utc
ORDER BY f.country_code, f.hour_utc;
