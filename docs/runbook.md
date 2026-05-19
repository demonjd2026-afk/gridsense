# GridSense Operational Runbook

This document is the day-to-day operational reference for running and
maintaining GridSense. It covers data refresh procedures, model
operations, agent debugging, cost management, and common issues with
their resolutions.

The architecture and design rationale live in
[architecture.md](architecture.md). Per-phase implementation details live
in the `PHASE*.md` files. This runbook is for *operating* what those
docs describe.

---

## 1. Current operational state

As of the latest update:

| Component | State | Schedule |
|---|---|---|
| 3 producers (Container Apps) | Running 24/7 | Continuous polling |
| 11 Databricks jobs (Bronze/Silver/Gold/ML) | **Paused** | Manual run only |
| Streamlit agent | Live at gridsense-carbon.streamlit.app | Always on (Streamlit Cloud) |
| LightGBM model | Registered as `ml.carbon_forecast_lgb v1` in UC | Last trained on 2026-05 backfill |
| Forecast fact table | `gold.fact_carbon_forecast` | 845 predictions, last refreshed manually |
| Azure OpenAI deployment | gpt-4.1-mini, swedencentral | Active |

The paused-jobs state is a deliberate FinOps choice for the portfolio
stage of this project. See section 6 (Cost Management) for the cost
rationale and how to re-enable hourly schedules.

---

## 2. Data refresh procedures

### When to refresh

- Before sending a wave of job applications, so the live demo shows
  recent data when recruiters click it.
- After more than ~2 weeks of data staleness — the agent's "today" and
  "tomorrow" answers start referencing weeks-old timestamps.
- If specific issues require fresh data to debug (e.g., reproducing an
  agent response with current grid conditions).

### Manual refresh sequence

All jobs are visible in the Databricks workspace at
`https://adb-7405606858641628.8.azuredatabricks.net/jobs`. Run in this
order — each layer depends on the previous one completing:

```text
1. bronze_uk_carbon_intensity       (~5 min)
2. bronze_open_meteo                 (~3 min)
3. bronze_entsoe                     (~5 min)

4. silver_uk_carbon_intensity        (~2 min)
5. silver_open_meteo                 (~2 min)
6. silver_entsoe                     (~3 min)
7. silver_grid_state                 (~3 min)

8. gold_dim_jobs (4 dims in parallel ok)  (~2 min)
9. gold_fact_carbon_intensity_30min  (~2 min)
10. gold_fact_generation_fuel_hourly (~3 min)
11. gold_fact_grid_hourly            (~3 min)

12. ml_features_carbon_forecast      (~2 min)
13. ml_infer_carbon_forecast         (~1 min)
```

Total wall-time: ~35 minutes. Cost per full refresh: ~₹50-100 in
Databricks Serverless compute.

### What NOT to re-run

- **`ml_train_carbon_forecast`** — only retrain when you have meaningfully
  more data or want to test feature changes. The current model
  (`ml.carbon_forecast_lgb v1`) was trained on a 2026-01 temporal split
  and is good for several months without retraining.

### Verifying the refresh worked

After the inference job completes:

```sql
-- In Databricks SQL editor
SELECT
  country_code,
  MAX(base_hour_utc) AS latest_base,
  MAX(target_hour_utc) AS latest_target,
  COUNT(*) AS predictions
FROM dbw_gridsense_dev.gold.fact_carbon_forecast
GROUP BY country_code
ORDER BY country_code;
```

Expected: 5 countries (DE, ES, FR, IT, NL), each with ~169 predictions
(7-day horizon), latest base_hour within the last 24h.

---

## 3. Model operations

### Current model

`ml.carbon_forecast_lgb v1` — LightGBM regressor predicting 24-hour-ahead
carbon intensity for 5 EU countries. Trained on ~130K rows of features
derived from `gold.fact_grid_hourly`. Test R² = 0.83 overall.

### When to retrain

| Trigger | Action |
|---|---|
| You changed features in `databricks/src/ml/features_carbon_forecast.py` | Re-run features → retrain |
| You changed model hyperparams in `train_carbon_forecast.py` | Retrain only |
| You have >1 month additional recent data and want better accuracy | Optional retrain |
| Per-country accuracy gets significantly worse (validate via per-country R²) | Investigate features first, then retrain |

### How to retrain

1. Open Databricks workspace
2. Navigate to job `ml_train_carbon_forecast`
3. Click **Run now**
4. Wait ~5-10 minutes for completion
5. Check MLflow experiment at `/Shared/gridsense_carbon_forecast`:
   - New run should appear with metrics (MAE, RMSE, R², MAPE)
   - Per-country breakdown logged as params
6. If metrics improve, register the new version:
   - In the MLflow run UI, click **Register model**
   - Select `dbw_gridsense_dev.ml.carbon_forecast_lgb`
   - Promote to `@Production` alias (or update the version reference in
     `databricks/src/ml/infer_carbon_forecast.py`)
7. Re-run `ml_infer_carbon_forecast` to materialize predictions from the
   new model version

### Honest model weaknesses

The model has uneven per-country accuracy:

| Country | Test R² | Notes |
|---|---|---|
| Italy | 0.75 | Strong — predictable patterns |
| France | 0.55 | Decent — nuclear-dominated, less volatile |
| Spain | 0.49 | Moderate |
| Germany | 0.38 | Weak — high wind/solar volatility |
| Netherlands | 0.20 | Very weak — same as Germany, smaller grid |

This is documented honestly in `docs/PHASE8.md`. The natural follow-up
work would be per-country sub-models or richer weather-derivative
features (wind forecast error, solar cloud rate-of-change). Tracked as
Phase 8.E if pursued.

---

## 4. GenAI agent operations

The agent is the Streamlit app at `streamlit_app/` deployed to Streamlit
Community Cloud. See [streamlit_app/README.md](../streamlit_app/README.md)
for the detailed app reference.

### Current configuration

| Setting | Value | Where set |
|---|---|---|
| LLM | Azure OpenAI gpt-4.1-mini (swedencentral) | `.streamlit/secrets.toml` |
| Backend | Databricks SQL Warehouse | `.streamlit/secrets.toml` |
| Per-session rate limit | 10 questions | `streamlit_app/agent/tools.py` |
| Daily global rate limit | 200 questions | `streamlit_app/app.py` |
| Tool count | 6 (5 SQL + 1 forecast) | `streamlit_app/agent/tools.py` |

### Adjusting rate limits

Open `streamlit_app/app.py` and `streamlit_app/agent/tools.py` and locate
the rate limit constants. After editing, push to main — but note the
deploy caveat below.

### Streamlit Cloud deploy caveat (important)

**Streamlit Cloud's auto-deploy does NOT always restart the Python
process.** This was discovered during Phase 8.D.4 debugging.

Symptoms:
- You push a code change to main
- The Streamlit Cloud logs show "🔄 Updated app!"
- But the agent's behavior still reflects the OLD code (e.g., old prompt
  text, old rate limits, old tool descriptions)

Fix:
1. Go to https://share.streamlit.io
2. Find the GridSense app
3. Click the three-dot menu
4. Click **Reboot app**

This forces a full process restart across all replicas. Required for
changes to module-level state (imports, constants, system prompts).
Hot-reload alone is not enough.

### Debugging a wrong agent answer

If a user reports the agent gave a wrong answer:

1. **Check tool transparency footer** — every answer has an expandable
   "Show data source used (N)" footer. Look at which tool was called and
   what it returned.
2. **Check data freshness** — if the agent answered about "today" but
   `gold.fact_carbon_intensity_30min` hasn't been refreshed recently, the
   answer used stale data. Trigger a manual refresh (section 2).
3. **Check forecast fact** — if the agent gave a forecast, verify
   `gold.fact_carbon_forecast` has recent base_hour timestamps.
4. **Check Streamlit Cloud logs** — `https://share.streamlit.io/` →
   app → **Manage app** → **Logs**. Errors during tool calls appear here.

---

## 5. Common operational scenarios

### "I need fresh data in the demo"

If `gold.fact_carbon_intensity_30min` or `gold.fact_carbon_forecast`
hasn't been refreshed recently, the agent's "today" and "tomorrow"
answers will reference stale timestamps. To refresh:

1. Run the manual job sequence (section 2 above) — ~35 minutes wall-time
2. Verify with the SQL query in section 2 ("Verifying the refresh worked")
3. Re-test the agent at https://gridsense-carbon.streamlit.app with
   any of the example questions in the live demo sidebar

Cost: ~₹50-100 per full refresh in Databricks Serverless compute.

### "I want to showcase the system to a stakeholder"

1. Refresh data the day before (section 2)
2. Walk through `docs/architecture-diagrams/gridsense-architecture.png`
   to explain the four-stage pipeline
3. Open https://gridsense-carbon.streamlit.app live
4. Demonstrate grounding: ask a question, expand "Show data source used"
   to reveal the SQL tool calls
5. Demonstrate forecasting: ask "What does the model predict for
   tomorrow's grid?" to show multi-country parallel orchestration
6. For technical follow-up, point to `docs/PHASE8.md` (ML rationale)
   and `docs/architecture.md` (design decisions + production gotchas)


### "The agent is returning errors"

Possible causes in priority order:
1. **Azure OpenAI quota exhausted** — check usage in Azure portal at
   the gpt-4.1-mini deployment
2. **Databricks SQL warehouse not available** — check `dbw_gridsense_dev`
   workspace, ensure warehouse is on (it auto-resumes on first query,
   but the first query can take ~30s)
3. **Stale code on Streamlit Cloud** — try a manual reboot (section 4
   caveat)
4. **Daily rate limit hit** — global daily limit (200) shared across all
   users; resets at UTC midnight

### "I want to enable continuous hourly job runs"

```text
1. In each job YAML under databricks/resources/jobs/, change:
     schedule:
       quartz_cron_expression: "0 0 0 0 0 ? 2099"  # paused
   to:
     schedule:
       quartz_cron_expression: "0 5 * * * ?"  # hourly at :05
2. Re-deploy with: databricks bundle deploy
3. Expected monthly cost: ₹2,500-3,500 for full hourly cadence
```

To pause again, reverse the change.

---

## 6. Cost management (FinOps)

### Cost components

| Component | Current state | Cost (paused mode) | Cost (active mode) |
|---|---|---|---|
| 3 Container Apps producers | Running 24/7 | ~₹2,200/month | ~₹2,200/month |
| 11 Databricks jobs | Paused | ~₹0 | ~₹2,500-3,500/month |
| Azure OpenAI (agent) | Rate-limited | ~₹100-500/month | ~₹100-500/month |
| Streamlit Cloud | Free tier | ₹0 | ₹0 |
| Storage (Delta Lake) | ~50GB | ~₹100/month | ~₹100/month |
| **Total** | **Current** | **~₹2,400/month** | **~₹4,900-6,300/month** |

### Why the producers run continuously even when jobs are paused

The producers cost ~₹2,200/month combined and serve two purposes:
1. The continuous-stream story is genuine (data IS being captured)
2. When jobs are unpaused, they have fresh data immediately

Pausing producers would save ~₹2,200/month but break the
"24/7 streaming pipeline" narrative. Trade-off accepted for portfolio.

### How to stop all costs (emergency)

If costs need to go to ~₹0 (e.g., after accepting an offer):

```text
1. Pause all jobs (already done) ✓
2. Stop the 3 Container Apps:
   - Azure Portal → ca-carbon-intensity-dev → Stop
   - Repeat for ca-open-meteo-dev and ca-entsoe-dev
3. Optionally delete the Databricks SQL warehouse if not needed
4. Streamlit Cloud stays free; can leave running
```

Net cost after emergency stop: ~₹100/month (Delta Lake storage only).

---

## 7. Known gotchas and lessons learned

These were discovered during build and operation. Capturing them here
so future-you (or contributors) don't re-debug:

### `%pip install` triggers kernel restart in Databricks Serverless
- Symptom: notebook widget values are lost after a `%pip install` cell
- Fix: put `%pip install` in the first cell; re-read widgets in a second
  cell immediately after

### ENTSO-E A75 returns empty for GB (post-Brexit)
- Symptom: backfill for GB country code returns no data
- Fix: documented as permanent limitation; live producer uses a
  different ENTSO-E endpoint that does return GB data

### ENTSO-E multi-Period XML parsing
- Symptom: `xmltodict` returns a dict for single Period, list for multi
- Fix: normalize to list before iterating

### OpenAI tool routing is dominated by tool descriptions, not prompts
- Symptom: agent kept asking "which country?" despite 3 iterations of
  system-prompt tightening
- Fix: rewrote the OpenAI function description in the tool schema; the
  LLM weighs descriptions more heavily than the system prompt for
  tool-selection decisions
- Lesson: put routing guidance directly in tool descriptions

### Streamlit Cloud auto-deploy does NOT always restart the process
- Symptom: code change pushed, logs show "Updated", but old behavior
  persists
- Fix: manual "Reboot app" in Streamlit Cloud dashboard
- Lesson: hot-reload doesn't restart all replicas; module-level state
  changes need full reboot

### macOS Finder creates `.DS_Store` everywhere
- Symptom: `.DS_Store` files appear in directories you've opened
- Fix: `.gitignore` already excludes them; ensure no accidental commits

---

## 8. Quick command reference

```bash
# Run the agent locally
cd streamlit_app
streamlit run app.py

# Deploy a Databricks Asset Bundle update
cd databricks
databricks bundle deploy --target dev

# Trigger a specific job manually via CLI
databricks jobs run-now --job-id <id>

# Check the latest forecast in SQL warehouse
# (run in Databricks SQL editor)
SELECT * FROM dbw_gridsense_dev.gold.fact_carbon_forecast
ORDER BY generated_at DESC LIMIT 10;

# Reboot the Streamlit Cloud app
# https://share.streamlit.io → app → ⋯ → Reboot app

# View Streamlit logs
# https://share.streamlit.io → app → Manage app → Logs
```

---

## 9. Who to ask

This is a portfolio project; there's no on-call rotation. If something
breaks and the architecture/PHASE docs don't help, the closure docs
in `docs/PHASE*.md` capture the design intent for each component.

For deep questions about specific decisions, the git log (`git log
--oneline`) reads as a coherent narrative of why things are the way
they are.
