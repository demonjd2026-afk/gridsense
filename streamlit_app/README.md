# GridSense Carbon Briefing Agent

A Streamlit web app that answers natural-language questions about EU and UK
grid carbon intensity. Uses Azure OpenAI (gpt-4.1-mini) with six SQL-backed
tools that query the GridSense lakehouse on Databricks Unity Catalog —
including a LightGBM-powered 24-hour forecast tool.

Live at: [gridsense-carbon.streamlit.app](https://gridsense-carbon.streamlit.app)

## Architecture

```
User question
    │
    ▼
Streamlit chat UI
    │
    ▼
gpt-4.1-mini (Azure OpenAI, swedencentral)
    │  decides which tool(s) to call
    │  can orchestrate multiple parallel tool calls
    ▼
One of 6 tools:
    - get_eu_carbon_rankings    →  gold.fact_grid_hourly
    - get_uk_regional_carbon    →  gold.fact_carbon_intensity_30min
    - get_country_fuel_mix      →  gold.fact_generation_fuel_hourly
    - get_24h_carbon_trend      →  gold.fact_grid_hourly
    - get_cleanest_window_uk    →  gold.fact_carbon_intensity_30min
    - get_carbon_forecast       →  gold.fact_carbon_forecast (ML predictions)
    │
    ▼
Databricks SQL Warehouse (serverless, auto-resumes)
    │
    ▼
Tool result fed back to the LLM
    │
    ▼
Natural-language answer with citations
    │
    └──→ "Show data source used (N)" expander reveals exact tool calls
```

## The six tools in detail

| Tool | Returns | Backing table |
|---|---|---|
| `get_eu_carbon_rankings` | Current carbon ranking across 5 EU countries | `gold.fact_grid_hourly` |
| `get_uk_regional_carbon` | Carbon intensity for a UK region (or all regions) | `gold.fact_carbon_intensity_30min` |
| `get_country_fuel_mix` | Generation breakdown by fuel type for a country | `gold.fact_generation_fuel_hourly` |
| `get_24h_carbon_trend` | 24-hour carbon time series for a country | `gold.fact_grid_hourly` |
| `get_cleanest_window_uk` | Optimal 4-hour window for low-carbon UK activity | `gold.fact_carbon_intensity_30min` |
| `get_carbon_forecast` | 24-hour-ahead ML forecast for a country (Phase 8) | `gold.fact_carbon_forecast` |

The forecast tool is the Phase 8 addition. It queries pre-computed
predictions from a LightGBM model (R²=0.83 on test), enabling questions
like *"Will Germany be cleaner tomorrow than today?"*.

## Run locally

```bash
cd streamlit_app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Fill in secrets
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit .streamlit/secrets.toml with real Azure OpenAI + Databricks values

streamlit run app.py
```

App opens at `http://localhost:8501`. First query may take ~30 seconds
while the Databricks Serverless warehouse warms up.

## Deploy to Streamlit Community Cloud

1. Push this repo to GitHub (already done if you're reading this).
2. Go to [share.streamlit.io](https://share.streamlit.io) and sign in
   with GitHub.
3. Click **New app**:
   - Repository: `demonjd2026-afk/gridsense`
   - Branch: `main`
   - Main file path: `streamlit_app/app.py`
4. In **Advanced settings → Secrets**, paste the contents of your
   `.streamlit/secrets.toml` (the real one, not the example).
5. Deploy. URL will be something like
   `https://gridsense-<hash>.streamlit.app`.

### ⚠️ Important deploy caveat

**Streamlit Cloud auto-deploy does NOT always restart the Python
process.** After pushing a code change, you may see "🔄 Updated app!"
in the logs but the OLD behavior persists in production.

The fix: click **"Reboot app"** in the Streamlit Cloud dashboard
(`https://share.streamlit.io` → your app → three-dot menu → Reboot).

This is required when changes affect module-level state — system
prompts loaded at import time, rate limit constants, tool definitions.
Hot-reload alone is not sufficient.

Discovered during Phase 8.D.4 debugging; documented in
[../docs/runbook.md](../docs/runbook.md) and
[../docs/architecture.md](../docs/architecture.md).

## Rate limits

The agent enforces two rate limits to bound Azure OpenAI costs:

| Limit | Value | Scope | Why |
|---|---|---|---|
| Per-session | 10 questions | Browser session | Prevents one user from exhausting budget |
| Daily global | 200 questions | All users, UTC day | Total daily token cap |

When a limit is hit, the agent gracefully refuses further questions
until the next session/day. The sidebar shows current usage.

To adjust limits: edit `agent/tools.py` (per-session) or `app.py`
(daily). After deploy, **reboot the app** (see caveat above) for
changes to take effect.

## Example questions

The agent excels at these patterns:

- **Current state**: "What's the cleanest EU country right now?"
- **Forecasts** (Phase 8): "Will Germany be cleaner tomorrow than today?"
- **Multi-country**: "What does the model predict for tomorrow's grid?"
- **Explanations**: "Why is Germany so dirty right now?"
- **UK-specific**: "Which UK region is cleanest?"
- **Trends**: "How has France's carbon trended over the last 24 hours?"
- **Optimization**: "When should I run my UK batch job for lowest carbon?"

The agent should call exactly one tool per single-country question.
For multi-country forecast questions ("the grid", "all of EU"), it
orchestrates 5 parallel `get_carbon_forecast` calls and synthesizes
the results.

## Files in this folder

```
streamlit_app/
├── README.md            # This file
├── app.py               # Streamlit UI and rate limit enforcement
├── requirements.txt     # Python dependencies
└── agent/
    ├── tools.py         # 6 SQL tool definitions + OpenAI function schemas
    └── prompts.py       # SYSTEM_PROMPT for tool routing
```

## Cost notes (FinOps)

| Cost component | Approximate |
|---|---|
| Azure OpenAI per question | ~₹1-3 (~$0.01-0.03) |
| Daily budget (200 questions × ₹2 avg) | ~₹400/day worst case |
| Realistic daily usage (portfolio demo) | ~₹20-50/day |
| Databricks SQL warehouse (when queried) | ~₹5-10 per query (serverless auto-resume) |

Full cost breakdown for the entire GridSense project is in
[../docs/runbook.md](../docs/runbook.md).

## Related docs

- [../docs/PHASE9.md](../docs/PHASE9.md) — Phase 9 agent implementation
- [../docs/PHASE8.md](../docs/PHASE8.md) — Phase 8 ML forecasting (the
  forecast tool's training pipeline)
- [../docs/runbook.md](../docs/runbook.md) — Operational runbook
- [../docs/architecture.md](../docs/architecture.md) — System architecture
