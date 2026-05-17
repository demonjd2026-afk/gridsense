-- ============================================================================
-- Phase 8.B verification — ENTSO-E generation backfill
-- ----------------------------------------------------------------------------
-- Run AFTER:
--   1. backfill_entsoe job completes (writes ~150K rows to bronze.entsoe)
--   2. silver_entsoe re-runs to MERGE new Bronze rows into silver.generation
--   3. (optional) gold facts that depend on generation re-run
--
-- Expected magnitudes (3yr × 6 countries × 24 hours):
--   - Per country per year: ~8,760 rows (24 × 365)
--   - 6 countries × 3 years: ~157,680 rows max, realistically ~145-155K
--     (some hours legitimately have no published data)
-- ============================================================================

-- 1. Bronze: live vs backfill split by source
SELECT
  get_json_object(envelope_json, '$.source') AS source,
  COUNT(*)                                   AS rows,
  COUNT(DISTINCT kafka_key)                  AS distinct_countries,
  MIN(event_date)                            AS earliest_date,
  MAX(event_date)                            AS latest_date
FROM dbw_gridsense_dev.bronze.entsoe
GROUP BY source
ORDER BY source;

-- 2. Silver: post-MERGE total
SELECT
  COUNT(*)                                          AS row_count,
  COUNT(DISTINCT country_code, period_start)        AS distinct_natural_keys,
  COUNT(DISTINCT country_code)                      AS distinct_countries,
  MIN(period_start)                                 AS earliest,
  MAX(period_start)                                 AS latest,
  DATEDIFF(DAY, MIN(period_start), MAX(period_start)) AS span_days
FROM dbw_gridsense_dev.silver.generation;

-- 3. Silver: per-country, per-year coverage
SELECT
  country_code,
  YEAR(period_start)            AS year,
  COUNT(*)                      AS hours,
  ROUND(AVG(total_generation_mw))  AS avg_total_mw,
  ROUND(MAX(total_generation_mw))  AS peak_mw,
  ROUND(MIN(total_generation_mw))  AS min_mw
FROM dbw_gridsense_dev.silver.generation
GROUP BY country_code, YEAR(period_start)
ORDER BY country_code, year;

-- 4. Silver: PSR type coverage (which fuels appear, how often)
SELECT
  country_code,
  pm.psr_type,
  pm.name                       AS psr_name,
  COUNT(*)                      AS hours_with_data,
  ROUND(AVG(pm.value_mw))        AS avg_mw
FROM dbw_gridsense_dev.silver.generation
LATERAL VIEW EXPLODE(generation_mix) m AS pm
GROUP BY country_code, pm.psr_type, pm.name
HAVING COUNT(*) > 100
ORDER BY country_code, avg_mw DESC;
