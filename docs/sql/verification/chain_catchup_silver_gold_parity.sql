-- ============================================================================
-- Chain catch-up — Silver/Gold row-count parity
-- ----------------------------------------------------------------------------
-- Purpose: After unpausing schedules (or a manual chain re-run after a paused
-- window), confirm Silver and Gold are in sync. For fact_carbon_intensity_30min
-- the mapping is 1:1 (one Gold row per Silver row), so row counts and latest
-- timestamps must match exactly.
-- When to use: After pause-then-resume cycles. Validates the FinOps pause
-- pattern documented in docs/architecture.md.
-- Expected: silver_carbon_rows == gold_carbon_rows, silver_latest == gold_latest.
-- ============================================================================

SELECT
  (SELECT COUNT(*) FROM dbw_gridsense_dev.silver.carbon_intensity) AS silver_carbon_rows,
  (SELECT COUNT(*) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min) AS gold_carbon_rows,
  (SELECT MAX(period_start) FROM dbw_gridsense_dev.silver.carbon_intensity) AS silver_latest,
  (SELECT MAX(period_start) FROM dbw_gridsense_dev.gold.fact_carbon_intensity_30min) AS gold_latest;
