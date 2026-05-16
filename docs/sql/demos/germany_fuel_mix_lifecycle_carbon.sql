-- ============================================================================
-- DEMO — Germany fuel mix and lifecycle carbon
-- ----------------------------------------------------------------------------
-- Pairs with the UK regional query as a cross-country complement. Shows
-- a single country's fuel mix at the latest hour, ranked by MW, with the
-- lifecycle CO2 implication of each fuel. Lignite typically dominates
-- both MW *and* emissions; wind generates similar MW with ~135x lower
-- per-MWh impact.
--
-- Uses dim_fuel_type.typical_gco2_per_kwh (IPCC AR5 lifecycle medians)
-- joined via fact_generation_fuel_hourly. This is the lifecycle-CO2
-- complement to the measured UK intensity in fact_carbon_intensity_30min.
-- ============================================================================

SELECT
  f.country_code,
  f.fuel_category,
  f.fuel_display_name,
  f.hour_utc,
  ROUND(f.value_mw, 1) AS mw,
  f.typical_gco2_per_kwh,
  ROUND(f.estimated_gco2_per_hour / 1000, 1) AS estimated_tco2_per_hour
FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly f
WHERE f.country_code = 'DE'
  AND f.time_key = (SELECT MAX(time_key) FROM dbw_gridsense_dev.gold.fact_generation_fuel_hourly WHERE country_code = 'DE')
ORDER BY f.value_mw DESC;
