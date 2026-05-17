# Dashboard 3 — GridSense — Lakehouse Health

Meta-dashboard. Proves the lakehouse is current and consistent before any
analytic dashboard is read.

## Datasets

| File | Rows | Purpose |
|---|---|---|
| `layer_row_counts.sql` | 14 | row counts per table |
| `freshness_per_table.sql` | 6 | latest business timestamp + age + status |
| `kpi_lakehouse_summary.sql` | 1 | total_rows, tables_tracked, layers_tracked |

## Widgets

1. Title + subtitle
2. 3 KPI counters
3. Row counts per table (left half)
4. Freshness per table (right half)
5. FinOps note text widget below — explains the stale status

## Screenshots

- `docs/screenshots/phase10/phase10-d3-1-lakehouse-health-overview.png`
- `docs/screenshots/phase10/phase10-d3-2-freshness-with-finops-note.png`

## The freshness-during-pause story

While the FinOps pause is active, every freshness row reads "stale" or "very
stale". This is intentional — the widget surfaces the lag instead of hiding it.
The text widget below the freshness table explains the cause. Two cohorts are
visible: tables ~21h old vs tables ~24h old. The 3-hour split exactly matches
the documented ENTSO-E publication lag.
