# Dashboard 1 — GridSense — UK Carbon Live

Intra-country dashboard. Same minute, 18 UK regions, 0–382 gCO₂/kWh spread.

## Datasets

| File | Rows | Purpose |
|---|---|---|
| `uk_latest_intensity.sql` | 18 | latest snapshot per region; ranking table + KPIs |
| `uk_intensity_24h_timeseries.sql` | ~864 | last 24h per region; line chart + filter |
| `uk_kpis_latest.sql` | 1 | cleanest/dirtiest/avg/GB national for KPI tiles |

## Widgets

1. Title + subtitle
2. 4 KPI counters (Cleanest | UK average | GB national | Dirtiest)
3. Ranking table (18 regions, sorted by intensity desc)
4. Multi-select region filter
5. 24h time-series line chart (filtered by widget 4)

## Screenshots

- `docs/screenshots/phase10/phase10-d1-1-uk-carbon-live-overview.png`
- `docs/screenshots/phase10/phase10-d1-2-uk-regional-24h-spread.png`
