-- Dashboard 1 — GridSense — UK Carbon Live
-- Dataset: uk_latest_intensity
--
-- Latest 30-min interval per UK region. 18 rows (14 DNO + 4 national rollups).
-- Powers: headline ranking table, KPI tiles, region multi-select filter.

SELECT
  region_id,
  region_name,
  region_type,
  period_start,
  intensity_forecast AS gco2_per_kwh,
  intensity_index,
  source_type,
  CASE region_id
    WHEN 1 THEN 57.5  WHEN 2 THEN 55.9  WHEN 3 THEN 53.7
    WHEN 4 THEN 54.9  WHEN 5 THEN 53.8  WHEN 6 THEN 53.2
    WHEN 7 THEN 51.6  WHEN 8 THEN 52.5  WHEN 9 THEN 52.9
    WHEN 10 THEN 52.2 WHEN 11 THEN 50.7 WHEN 12 THEN 51.3
    WHEN 13 THEN 51.5 WHEN 14 THEN 51.1 WHEN 15 THEN 52.5
    WHEN 16 THEN 56.5 WHEN 17 THEN 52.1 WHEN 18 THEN 54.5
  END AS approx_lat,
  CASE region_id
    WHEN 1 THEN -4.2  WHEN 2 THEN -3.9  WHEN 3 THEN -2.5
    WHEN 4 THEN -1.6  WHEN 5 THEN -1.5  WHEN 6 THEN -3.1
    WHEN 7 THEN -3.5  WHEN 8 THEN -1.9  WHEN 9 THEN -1.2
    WHEN 10 THEN 0.5  WHEN 11 THEN -3.7 WHEN 12 THEN -1.2
    WHEN 13 THEN -0.1 WHEN 14 THEN 0.5  WHEN 15 THEN -1.5
    WHEN 16 THEN -4.0 WHEN 17 THEN -3.8 WHEN 18 THEN -2.5
  END AS approx_lon
FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
WHERE period_start = (
  SELECT MAX(period_start) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
);
