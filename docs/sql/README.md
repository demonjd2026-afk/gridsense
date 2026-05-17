# GridSense SQL Library

Reusable SQL queries for the GridSense lakehouse on Unity Catalog
(`dbw_gridsense_dev`). Three categories:

## verification/

Data-quality and integrity queries. Run these after any pipeline
change to confirm nothing regressed. Every Phase-completion screenshot
in `docs/screenshots/` is sourced from one of these.

| File | What it checks |
|---|---|
| `phase7a_row_counts.sql` | All Gold tables have expected row counts |
| `phase7a_measure_quality.sql` | `fact_generation_fuel_hourly` measure nulls + range |
| `phase7b_fact_summary.sql` | `fact_carbon_intensity_30min` rows, grain, forecast vs actual |
| `phase7b_fact_grid_hourly_summary.sql` | `fact_grid_hourly` row count + grain + share percentages + per-country snapshot |
| `phase7b_orphan_check.sql` | Every fact FK resolves to a dim row |
| `chain_catchup_silver_gold_parity.sql` | Silver and Gold row-count parity after a re-run |
| `chain_catchup_all_layers_freshness.sql` | Latest timestamp per Silver table |

## demos/

Queries that tell the GridSense story. These are what gets
screenshotted for the README and walked through in interviews.

| File | What it shows |
|---|---|
| `uk_regional_carbon_spread.sql` | The headline: 0 vs 382 gCO₂/kWh across UK regions in the same minute |
| `germany_fuel_mix_lifecycle_carbon.sql` | Lignite emits 135× more per MWh than wind in Germany right now |
| `carbon_intensity_24h_per_region.sql` | 24h time-series of intensity per UK region — input to Phase 10 charts |

## exploration/

Ad-hoc queries kept around because they revealed something
unexpected or might be reused. Less polished than `verification/`
or `demos/`; populated as the project grows.

## Conventions

- All queries are fully qualified (`dbw_gridsense_dev.gold.fact_...`)
  so they run from any catalog context without modification.
- Each file starts with a comment block: purpose, expected output,
  when to use.
- Files are runnable as-is in Databricks SQL Editor — no parameter
  substitution required for the dev environment.

## dashboards/

SQL backups of the Databricks AI/BI Dashboard datasets. One folder per
dashboard, each containing the dataset `.sql` files and a `README.md`
describing the widgets and screenshots.

| Folder | Dashboard |
|---|---|
| `dashboards/d1_uk_carbon_live/` | GridSense — UK Carbon Live |
| `dashboards/d2_european_fuel_mix/` | GridSense — European Fuel Mix |
| `dashboards/d3_lakehouse_health/` | GridSense — Lakehouse Health |

See `docs/PHASE10.md` for the full design writeup.
