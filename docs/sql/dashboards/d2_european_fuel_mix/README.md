# Dashboard 2 — GridSense — European Fuel Mix

Cross-country dashboard. Six EU countries, same hour, structurally different
energy mixes drive an order-of-magnitude difference in lifecycle CO₂.

## Datasets

| File | Rows | Purpose |
|---|---|---|
| `eu_fuel_mix_latest_hour.sql` | ~20 | latest hour by (country, fuel_category) |
| `eu_kpis_latest_hour.sql` | 1 | 3 KPI tiles |
| `eu_co2_24h_per_country.sql` | ~119 | 24h time series by country |

## Widgets

1. Title + subtitle (mentions ENTSO-E ~3-4h publication lag)
2. 3 KPI counters
3. Country×fuel ranking table (sorted by tCO₂/hour desc)
4. Stacked bar chart (mix by country)
5. 24h CO₂ line chart by country — the headline visual

## Screenshots

- `docs/screenshots/phase10/phase10-d2-1-european-co2-24h-divergence.png`
- `docs/screenshots/phase10/phase10-d2-2-fuel-mix-by-country.png`

## Note on units (post Phase 7.C fix)

`gold.fact_generation_fuel_hourly.estimated_gco2_per_hour` is stored as
grams CO₂ per hour. All three dataset SQL files divide by `1e6` to convert
to tons/hour for display. Earlier versions of these files contained a
`* 1000` workaround because the upstream column was missing the MWh → kWh
conversion; that has now been fixed at the source (see `databricks/src/gold/
fact_generation_fuel_hourly.py`).
