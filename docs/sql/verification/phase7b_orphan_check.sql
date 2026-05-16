-- ============================================================================
-- Phase 7.B — Referential integrity check
-- ----------------------------------------------------------------------------
-- Purpose: Every fact FK must resolve to a dim row. Any orphan = silent
-- data-quality bug that would lose rows from BI queries.
-- When to use: After any change to dims or fact builds.
-- Expected: All three counts = 0.
-- ============================================================================

SELECT
  (SELECT COUNT(*) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min f
   LEFT JOIN dbw_gridsense_dev.gold.dim_uk_region r ON f.region_id = r.region_id
   WHERE r.region_id IS NULL)                                                AS orphan_region,
  (SELECT COUNT(*) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min f
   LEFT JOIN dbw_gridsense_dev.gold.dim_country c ON f.country_key = c.country_code
   WHERE c.country_code IS NULL)                                             AS orphan_country,
  (SELECT COUNT(*) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min f
   LEFT JOIN dbw_gridsense_dev.gold.dim_time t ON f.time_key = t.time_key
   WHERE t.time_key IS NULL)                                                 AS orphan_time;
