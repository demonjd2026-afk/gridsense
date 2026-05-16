-- ============================================================================
-- Phase 7.A — Gold table row counts
-- ----------------------------------------------------------------------------
-- Purpose: Confirm all 6 Gold tables exist and have expected row counts.
-- When to use: After any Gold job rebuild, or as part of a daily smoke check.
-- Expected output (as of Phase 7.B close, 2026-05-16):
--   dim_country                       6
--   dim_fuel_type                     29
--   dim_time                          17,521
--   dim_uk_region                     18
--   fact_carbon_intensity_30min       ~3,000+ (grows ~864/day)
--   fact_generation_fuel_hourly       ~3,000+ (grows ~144/day, 6 countries x 24h)
-- ============================================================================

SELECT 'dim_country'                  AS table_name, COUNT(*) AS rows FROM dbw_gridsense_dev.gold.dim_country
UNION ALL SELECT 'dim_fuel_type',                    COUNT(*)        FROM dbw_gridsense_dev.gold.dim_fuel_type
UNION ALL SELECT 'dim_time',                         COUNT(*)        FROM dbw_gridsense_dev.gold.dim_time
UNION ALL SELECT 'dim_uk_region',                    COUNT(*)        FROM dbw_gridsense_dev.gold.dim_uk_region
UNION ALL SELECT 'fact_carbon_intensity_30min',      COUNT(*)        FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min
UNION ALL SELECT 'fact_generation_fuel_hourly',      COUNT(*)        FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly
ORDER BY table_name;
