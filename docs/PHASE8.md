# Phase 8 — ML Forecasting: Carbon Intensity Prediction

A LightGBM regressor trained on 3 years of historical weather + generation +
carbon intensity data, registered in Unity Catalog Model Registry, generating
24-hour-ahead carbon intensity forecasts surfaced live through the GenAI
agent at [gridsense-carbon.streamlit.app](https://gridsense-carbon.streamlit.app).

## Why backfill before training

The original plan was *"wait two weeks for live producers to accumulate
training data."* That works, but it's slow and the resulting training set
covers only two weeks of seasonal variation — useless for a model that
needs to learn weekend-vs-weekday patterns, winter-vs-summer renewables,
and the rhythms of three years of grid behaviour.

Backfill collapses that wait to ~30 minutes of compute and produces a
dataset that's 100× larger and 50× more temporally rich.

The architectural split is lambda-style:

| Path | When | How |
|---|---|---|
| **Live ingestion** | Continuous | Producer → Event Hubs → Bronze (streaming) |
| **Historical backfill** | One-shot | API → Bronze direct (batch, skipping Event Hubs) |

Both land in the same Bronze Delta table. Silver MERGE on natural key
deduplicates any overlap. Source-tagged envelopes (`*-backfill`) preserve
the audit trail through Silver.

## Phase 8.A — UK Carbon Intensity backfill

Simplest of the three backfills: JSON response, no auth, single API
endpoint. The hard part was an undocumented API quirk.

### What got built

Notebook at `databricks/src/backfill/backfill_carbon_intensity.py`. Pulls
3 years of half-hourly UK regional carbon intensity for 18 distribution
network operators across 156 7-day chunks.

### The "two-week limit" trap

API documentation says *"up to two weeks per request."* That phrasing implies
14 days is supported; in practice the endpoint enforces a **strict less-than
14-day window**. A 14-day request returns:

```
{
  "error": {
    "code": "400 Bad Request",
    "message": "The date range you have specified is greater than 14 days."
  }
}
```

Found this the hard way at chunk 1 of the full backfill. Reduced chunk size
to 7 days, doubled the call count to 156, ran clean.

### Results

| Metric | Value |
|---|---|
| Bronze rows added | 940,464 |
| Silver rows after MERGE | 937,728 |
| Silver invalid / quarantined | 0 / 0 |
| Date range | 2023-05-16 → 2026-05-16 |
| Distinct regions | 18 |

The 3,000-row Silver < Bronze delta is dedup from a 1-week test chunk
overlapping with live data. Clean.

## Phase 8.B — ENTSO-E generation backfill

Hardest of the three. XML response, mandatory API token, per-country
queries, multi-Period TimeSeries inside each XML response, sub-hourly
Points that need bucketing into hours.

### What got built

Notebook at `databricks/src/backfill/backfill_entsoe.py`. Pulls 3 years
of hourly Actual Generation per Production Type (A75/A16) for 5 European
bidding zones (DE, FR, ES, IT, NL) across 36 30-day chunks × 5 countries
= 180 API calls.

### Three parser iterations before it worked

ENTSO-E's XML is one of those formats where the schema is intuitive
*after* you've fought it. The shape:

```
GL_MarketDocument
└── data (list, one entry per period)
    └── regions (list, one entry per region)
        └── intensity, generationmix
```

But each TimeSeries can have **multiple Periods** when the chunk spans
multiple days, which trips up a naive `series.get("Period", {})` —
`xmltodict` returns a list when there are multiple Periods, a dict when
there's one. Normalizing to a list and iterating fixed it on the third
parser attempt.

### GB is missing from historical data

Live producer includes GB; the backfill silently returns empty for GB.
Confirmed via direct curl that ENTSO-E doesn't publish A75 historical for
the GB control area (post-Brexit data-sharing change). Left as a
documented limitation rather than a defect. The other 5 countries are
fully covered.

### Results

| Metric | Value |
|---|---|
| Bronze rows added | 131,621 |
| Silver rows after MERGE | 131,453 |
| Silver invalid / quarantined | 0 / 0 |
| Gold fact_generation_fuel_hourly rows | 1,483,192 |
| Per country per year | ~8,784 (full year, leap) |
| Per country per year | ~8,760 (full year, non-leap) |

## Phase 8.C — Open-Meteo weather backfill

Simplest by far. JSON response, no auth, ~5 cities × 1 API call each =
**6 calls total** for the full 3-year window. The whole backfill ran
in under a minute.

### Why the Historical Forecast API, not ERA5

Open-Meteo offers two relevant archives:

| Endpoint | Backend | Best for |
|---|---|---|
| `historical-forecast-api.open-meteo.com` | Archived past forecasts | Training data that matches what live producers see |
| `archive-api.open-meteo.com` | ERA5 reanalysis | Ground-truth weather observations |

We picked the Historical Forecast API. Two reasons:

1. **Schema is byte-identical to the live producer's endpoint.** No field
   renames, no unit conversions, no type casts.
2. **Zero distributional shift between training and inference.** The model
   trains on archived forecast outputs and infers on live forecast outputs
   from the same models. Industry-standard pattern for forecast-correction
   pipelines.

ERA5 would have been more accurate as ground truth, but introduces a
train/inference mismatch we don't need.

### Results

| Metric | Value |
|---|---|
| Bronze rows added | ~157,680 |
| Silver rows after MERGE | ~157,000 |
| Gold fact_grid_hourly rows (after 8.C unblocked) | 131,453 |

The integrated fact `gold.fact_grid_hourly` was stuck at 352 rows after
8.A (it needs all three sources). 8.C unblocked the 3-way Silver join,
which propagated to Gold. **131,453 fully-joined rows are what Phase 8.D
trains on.**

## Phase 8.D.1 — Feature engineering

The training table needs more than raw lakehouse columns. `gold.feature_carbon_forecast`
adds:

| Feature group | Columns |
|---|---|
| **Current state** | temperature_c, wind_speed_kmh, cloud_cover_pct, solar_radiation_wm2, total_generation_mw, renewable_share_pct, low_carbon_share_pct, carbon_current |
| **Calendar** | hour_of_day, day_of_week, month, is_weekend |
| **Lag** | carbon_lag_1h, carbon_lag_24h, carbon_lag_168h |
| **Rolling 24h trailing** | carbon_rolling_24h_mean, temp_rolling_24h_mean |
| **Categorical** | country_code (passed as native LightGBM category) |
| **Target** | target_t24h (carbon intensity 24 hours from now) |

19 features → 1 target. All windowed operations partition by `country_code`
so each country's series stays independent of the others.

### Predictive signal before training

Correlations from `feature_carbon_forecast` revealed strong signal before
the model was ever trained:

| Country | renewable→carbon | low_carbon→carbon | carbon_lag_24h→current | current→target_t24h |
|---|---|---|---|---|
| DE | -0.988 | -0.990 | 0.655 | 0.655 |
| ES | -0.700 | -0.992 | 0.728 | 0.728 |
| FR | -0.210 | -0.982 | 0.726 | 0.726 |
| IT | -0.991 | -0.992 | 0.807 | 0.807 |
| NL | -0.937 | -0.948 | 0.501 | 0.502 |

Two interesting nuances:

- **`low_carbon_share_pct` is near-perfectly anti-correlated with carbon
  intensity across all countries** (-0.94 to -0.99). This is partly
  tautological — the carbon-intensity calculation uses low-carbon share
  as an input — but it tells us the model will lean heavily on this
  feature.
- **France's `renewable_share_pct` correlation is weak (-0.21)** while
  every other country shows strong negative correlation. Why? France runs
  nuclear baseload; nuclear is low-carbon but not "renewable." So renewable
  share doesn't predict French carbon — `low_carbon_share` does. This is
  why our dim table separated the two flags.

### Results

| Metric | Value |
|---|---|
| Source rows | 131,453 (fact_grid_hourly) |
| Feature rows after dropping NULL boundaries | 130,493 |
| Per country | ~26,100 rows |
| Date range | 2023-05-24 → 2026-05-15 |

NULL boundaries = first week of each country (lag_168h is NULL) and last
day (target_t24h is NULL). Dropped 960 rows total. Cleaner training
matters more than maximizing row count.

## Phase 8.D.2 — Model training

Trained a single global LightGBM regressor with country as a native
categorical feature.

### Why LightGBM, not XGBoost or deep learning

For 130K rows of tabular features:

1. **Tabular features** — 19 numeric/categorical columns. Trees excel here.
2. **Native categorical support** — `country_code` passed as a string;
   LightGBM uses its optimal-split algorithm internally. No one-hot
   encoding needed.
3. **Fast** — trains in under a minute on Databricks Serverless.
4. **Interpretable** — feature importance comes for free.
5. **No GPU** — pure CPU.

XGBoost would have given equivalent accuracy at ~2× training time.
Deep learning is overkill at this row count.

### Why a single global model, not per-country

With country as a categorical feature, LightGBM learns country-specific
patterns inside one model — the tree splits reach country-conditional
leaves naturally. The alternative — 5 separate models — quintuples the
operational surface (5 registrations, 5 inference paths, 5 versions to
retrain) for marginal accuracy gain.

Single model wins for portfolio simplicity. **The interview story is
cleaner**: *"trained a LightGBM on 130K hours across 5 countries with
country as a categorical feature."*

### Why point forecasts, not confidence intervals

Confidence intervals (p10/p50/p90) would have required training 3 separate
quantile regressors and complicating the agent's response logic. Marginal
portfolio impact. Cut. Phase 8.E (future) could add them.

### Train/test split

Temporal — no random shuffle. Mimics the real-world deployment scenario:
train on past data, predict on future data.

| Split | Date range | Rows |
|---|---|---|
| Train | 2023-05-24 → 2025-12-31 | 114,318 |
| Test | 2026-01-01 → 2026-05-15 | 16,175 |

### Results

```
=== Test set performance ===
MAE:  44.19 gCO2/kWh
RMSE: 68.00 gCO2/kWh
R²:   0.828
MAPE: 21.2%

=== Per-country performance ===
  DE: MAE= 79.21 gCO2/kWh (rel  22.2%) | R²=0.379 | avg target= 356.2
  ES: MAE= 20.91 gCO2/kWh (rel  18.5%) | R²=0.493 | avg target= 112.9
  FR: MAE=  7.00 gCO2/kWh (rel  20.5%) | R²=0.548 | avg target=  34.1
  IT: MAE= 31.69 gCO2/kWh (rel  10.8%) | R²=0.752 | avg target= 294.7
  NL: MAE= 81.85 gCO2/kWh (rel  21.2%) | R²=0.200 | avg target= 386.3
```

### Reading the results honestly

The headline R² of 0.83 is partly cross-country variance — the model
trivially learns *"FR is cleaner than DE"* and that alone reduces overall
variance. The within-country R² is the more honest measure:

| Country | Within-country R² | Quality |
|---|---|---|
| IT | **0.752** | Strong — predictable gas-dominant grid |
| FR | 0.548 | OK — narrow range, low absolute error |
| ES | 0.493 | OK |
| DE | 0.379 | Weak — high renewable variance |
| NL | **0.200** | Very weak — high gas+wind variance |

The DE and NL gaps are real. These grids have higher hour-to-hour
volatility because of wind and solar swings; the current feature set
doesn't capture wind-forecast-error or solar-cloud-rate-of-change well.
**This is the Phase 8.E follow-up:** per-country models or richer weather
derivatives could meaningfully improve DE/NL.

### Top features by gain

| Rank | Feature | Importance |
|---|---|---|
| 1 | `carbon_current` | 1.9×10¹⁰ (dominant) |
| 2 | `low_carbon_share_pct` | 1.2×10⁹ |
| 3 | `country_code` | 1.1×10⁹ |
| 4 | `carbon_lag_24h` | 4.0×10⁸ |
| 5 | `day_of_week` | 3.3×10⁸ |

The model relies most on the current value (carbon persistence is strong
at 24h horizons) followed by the structural country/share features. Weather
features rank lower — they explain marginal variance once you know
where and when you are.

### Model artifact

Registered in Unity Catalog as `dbw_gridsense_dev.ml.carbon_forecast_lgb`
version 1. Signature inferred from a 5-row training sample. The MLflow
experiment lives at `/Shared/gridsense_carbon_forecast`.

## Phase 8.D.3 — Inference + Gold table

The model becomes useful only when its predictions are materialized
somewhere the agent can query. `gold.fact_carbon_forecast` is that table.

### Why MERGE, not append

Predictions get refreshed when the model is retrained or when feature
values get back-published. MERGE on natural key `(country_code,
base_hour_utc)` ensures the latest prediction per (country, hour) wins;
re-running inference is idempotent.

### Why 7 days of predictions, not just the current hour

The agent benefits from historical context. A user asking *"how have
forecasts changed this week?"* needs more than one row per country. 7
days × 5 countries × 24 hours = 840 rows is cheap to compute and
materialize.

### Schema

| Column | Type | Purpose |
|---|---|---|
| `country_code` | STRING | Natural-key part 1 |
| `base_hour_utc` | TIMESTAMP | Natural-key part 2 (when prediction was made) |
| `target_hour_utc` | TIMESTAMP | Always = base + 24h |
| `horizon_h` | INT | Always 24 (single-horizon design) |
| `predicted_carbon_gco2_kwh` | DOUBLE | The model's prediction |
| `carbon_current_at_base` | DOUBLE | Carbon when prediction was made, for context |
| `model_name` | STRING | Fully-qualified registered model |
| `model_version` | STRING | Specific version used |
| `generated_at` | TIMESTAMP | When inference ran |

### Results

| Metric | Value |
|---|---|
| Forecast rows | 845 |
| Distinct countries | 5 |
| Window | last 7 days |
| Latest prediction (DE) | 366.3 gCO₂/kWh (current 395.4, expected -7%) |
| Latest prediction (FR) | 26.5 gCO₂/kWh (current 21.0, +26%) |

## Phase 8.D.4 — Agent integration

The smallest code phase by line count, the largest by portfolio impact.

### One new tool, three files touched

A 6th tool added to the agent: `get_carbon_forecast(country_code)`. The
agent already had 5 tools for querying current state; the new tool queries
`gold.fact_carbon_forecast` for the latest prediction per country.

| File | Change |
|---|---|
| `streamlit_app/agent/tools.py` | Function + OpenAI schema + registry entry |
| `streamlit_app/agent/prompts.py` | System prompt extended to mention forecast capability + per-country accuracy nuance |
| `streamlit_app/app.py` | One new sidebar example question |

### The live demo flow

A user lands on `gridsense-carbon.streamlit.app` and asks:

> *"Should I run my batch job in Germany tomorrow or France?"*

The agent:

1. Recognizes this as a forecast question (system prompt update)
2. Calls `get_carbon_forecast(country_code='DE')` and `get_carbon_forecast(country_code='FR')` (autonomous tool orchestration)
3. Compares the predictions
4. Replies: *"France will stay much cleaner — ~26 gCO₂/kWh vs Germany's ~366. Run in France."*
5. Shows the *"Show data source used (2)"* expander that reveals both tool calls

That entire interaction is **2 SQL queries against a Gold fact table
holding pre-computed LightGBM predictions** — but to the user it looks
like the agent "knows" things. That's the magic of grounding GenAI in
real data.

### What this does NOT do

- **No model serving endpoint.** The agent queries the *predictions*, not
  the model directly. Inference is batch (every run of the 8.D.3 notebook
  materializes 7 days of predictions). If predictions become stale,
  re-running the inference notebook refreshes them.
- **No retraining loop.** Phase 8.E would add a scheduled retrain when new
  data accumulates.

## The complete agent (post-Phase 8.D.4)

The agent now has 6 tools spanning live state + ML forecasts:

| # | Tool | Source | Purpose |
|---|---|---|---|
| 1 | `get_eu_carbon_rankings` | `fact_grid_hourly` | Current cleanest/dirtiest EU country |
| 2 | `get_uk_regional_carbon` | `fact_carbon_intensity_30min` | Current cleanest/dirtiest UK region |
| 3 | `get_country_fuel_mix` | `fact_generation_fuel_hourly` | Which fuels are driving emissions |
| 4 | `get_24h_carbon_trend` | `fact_grid_hourly` | Hour-by-hour past 24h trend |
| 5 | `get_cleanest_window_uk` | `fact_carbon_intensity_30min` | Best 30-min slot in upcoming UK forecast |
| 6 | **`get_carbon_forecast`** | **`fact_carbon_forecast`** | **24h-ahead ML prediction (NEW)** |

Tools 1-5 answer *what's happening now*. Tool 6 answers *what's going to
happen next*. Together they cover the carbon-aware-compute story: a user
can ask *"is now a good time to run, and if not, when?"*

## Limitations + Phase 8.E roadmap

Honest accounting of what's incomplete:

### Per-country accuracy gaps

DE and NL within-country R² is weak (0.38 and 0.20 respectively). Likely
causes:

- High wind/solar variance → more abrupt hour-to-hour swings
- Current features capture trailing weather averages, not weather forecast
  *errors* or *rates of change*
- Single global model may be averaging across structurally-different grid
  types

### What 8.E (if pursued) would do

| Idea | Cost | Expected gain |
|---|---|---|
| Per-country LightGBM (5 models) | +1 day | DE/NL R² → 0.5-0.6 likely |
| Hyperparameter tuning via Optuna | +1 day | Marginal — current params are reasonable defaults |
| Add `solar_radiation_lag_1h - lag_24h` rate-of-change features | +2 hours | Moderate gain for solar-heavy days |
| Add multi-horizon targets (t+1h, t+6h, t+12h, t+24h) | +1 day | Enables "best time today" questions |
| Quantile regression for confidence intervals | +1 day | Better honesty UX for high-uncertainty predictions |

For now: the model is shipped, the agent uses it, the limitations are
documented. Phase 8.E is a portfolio decision — pursue if hiring
conversations want depth on this specific area, otherwise the existing
end-to-end pipeline tells the story.

### GB historical data gap

ENTSO-E doesn't publish A75 historical for GB. Model trains on 5
countries (DE/ES/FR/IT/NL); the agent answers forecast questions for
those 5 only. GB is covered by Tool 2 and Tool 5 (UK Carbon Intensity API
has its own data path) — separate model, separate story.

### No automated retraining schedule

The MLflow model is registered as version 1. There's no scheduled job
to retrain weekly on new data. In a production setting, you'd add
a Databricks workflow that:

1. Re-runs `features_carbon_forecast` (incremental mode)
2. Re-trains LightGBM on the expanded data
3. Registers a new version
4. Updates the inference notebook to use `model_version=latest`

For portfolio purposes, manual retraining when meaningful new data
accumulates is fine.

## Files

| Path | Purpose |
|---|---|
| `databricks/src/backfill/backfill_carbon_intensity.py` | 8.A — UK CI backfill |
| `databricks/src/backfill/backfill_entsoe.py` | 8.B — ENTSO-E backfill |
| `databricks/src/backfill/backfill_open_meteo.py` | 8.C — Open-Meteo backfill |
| `databricks/src/ml/features_carbon_forecast.py` | 8.D.1 — feature engineering |
| `databricks/src/ml/train_carbon_forecast.py` | 8.D.2 — LightGBM training + MLflow |
| `databricks/src/ml/infer_carbon_forecast.py` | 8.D.3 — inference → Gold |
| `streamlit_app/agent/tools.py` | 8.D.4 — agent tool added |
| `docs/sql/verification/phase8a_backfill_summary.sql` | Verification queries |
| `docs/sql/verification/phase8b_backfill_summary.sql` | Verification queries |
| `docs/sql/verification/phase8c_backfill_summary.sql` | Verification queries |
| `docs/sql/verification/phase8d1_features_summary.sql` | Verification queries |
| `docs/sql/verification/phase8d2_training_summary.sql` | Verification queries |
| `docs/sql/verification/phase8d3_inference_summary.sql` | Verification queries |

## Commits in this phase

```
3d4ccc7  feat(agent):    Phase 8.D.4 — add ML carbon forecast tool to agent (6 tools, live ML predictions)
bec1f57  feat(ml):       Phase 8.D.3 — inference + gold.fact_carbon_forecast (845 predictions, 5 countries × 7 days)
acf14c0  feat(ml):       Phase 8.D.2 — LightGBM training + MLflow registration (R²=0.83, registered as ml.carbon_forecast_lgb)
1ff2724  feat(ml):       Phase 8.D.1 — feature engineering for carbon forecast (130K rows, 19 features)
3d838b6  feat(backfill): Phase 8.B + 8.C — ENTSO-E and Open-Meteo historical backfills
37cb09d  feat(backfill): Phase 8.A — UK Carbon Intensity 3yr historical backfill
```

Six substantive commits across two days. Each shipped independently;
each was verified end-to-end before the next started.
