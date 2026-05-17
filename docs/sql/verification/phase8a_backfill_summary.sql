-- ============================================================================
-- Phase 8.A verification — UK Carbon Intensity backfill
-- ----------------------------------------------------------------------------
-- Run AFTER:
--   1. backfill_carbon_intensity job completes
--   2. silver_carbon_intensity job re-runs to MERGE the new Bronze rows
--   3. gold_fact_carbon_intensity_30min job re-runs to refresh Gold
--
-- Expected (3-year backfill, 18 regions, 30-min periods):
--   - bronze rows post-backfill: ~946,080 backfill + however many live rows
--   - silver rows post-merge:     ~946,080 (dedup'd to natural key)
--   - earliest period_start:      ~2023-05-17 00:00 UTC
--   - latest period_start:        whenever live producer last published
--   - distinct regions:           18
-- ============================================================================

-- 1. Bronze: backfill vs live source split
SELECT
  get_json_object(envelope_json, '$.source')   AS source,
  COUNT(*)                                     AS row_count,
  MIN(event_date)                              AS earliest_date,
  MAX(event_date)                              AS latest_date,
  COUNT(DISTINCT kafka_key)                    AS distinct_regions
FROM dbw_gridsense_dev.bronze.carbon_intensity
GROUP BY source
ORDER BY source;

-- 2. Silver: total rows, time span, region coverage post-MERGE
SELECT
  COUNT(*)                                     AS row_count,
  COUNT(DISTINCT region_code, period_start)    AS distinct_natural_keys,
  COUNT(DISTINCT region_code)                  AS distinct_regions,
  MIN(period_start)                            AS earliest,
  MAX(period_start)                            AS latest,
  DATEDIFF(DAY, MIN(period_start), MAX(period_start)) AS span_days
FROM dbw_gridsense_dev.silver.carbon_intensity;

-- 3. Silver: rows per year (sanity-check coverage)
SELECT
  YEAR(period_start)           AS year,
  COUNT(*)                     AS rows,
  COUNT(DISTINCT region_code)  AS regions,
  MIN(period_start)            AS first_period,
  MAX(period_start)            AS last_period
FROM dbw_gridsense_dev.silver.carbon_intensity
GROUP BY YEAR(period_start)
ORDER BY year;

-- 4. Gold: fact table updated count
SELECT
  COUNT(*)                                     AS row_count,
  MIN(period_start)                            AS earliest,
  MAX(period_start)                            AS latest,
  COUNT(DISTINCT region_name)                  AS distinct_regions
FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min;
