-- ============================================================================
-- Chain catch-up — Latest timestamp per Silver table
-- ----------------------------------------------------------------------------
-- Purpose: Confirm all 4 Silver tables are caught up to ~current time after
-- a chain re-run. If one lags significantly, that layer's job failed or
-- didn't run.
-- When to use: After unpausing schedules or a manual chain re-run.
-- Note: silver.weather doesn't have period_start; using ingested_at instead.
-- ============================================================================

SELECT 'silver.carbon_intensity' AS table_name, COUNT(*) AS rows, MAX(period_start) AS latest FROM dbw_gridsense_dev.silver.carbon_intensity
UNION ALL
SELECT 'silver.generation', COUNT(*), MAX(period_start) FROM dbw_gridsense_dev.silver.generation
UNION ALL
SELECT 'silver.weather', COUNT(*), MAX(ingested_at) FROM dbw_gridsense_dev.silver.weather
UNION ALL
SELECT 'silver.grid_state', COUNT(*), MAX(hour_utc) FROM dbw_gridsense_dev.silver.grid_state
ORDER BY table_name;
