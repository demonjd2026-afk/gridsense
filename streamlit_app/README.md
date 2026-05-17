# GridSense Carbon Briefing Agent

A Streamlit web app that answers natural-language questions about EU and UK
grid carbon intensity. Uses Azure OpenAI (gpt-4.1-mini) with five SQL-backed
tools that query the GridSense lakehouse on Databricks Unity Catalog.

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
    ▼
One of 5 SQL tools:
    - get_eu_carbon_rankings    →  fact_grid_hourly
    - get_uk_regional_carbon    →  fact_carbon_intensity_30min
    - get_country_fuel_mix      →  fact_generation_fuel_hourly
    - get_24h_carbon_trend      →  fact_grid_hourly
    - get_cleanest_window_uk    →  fact_carbon_intensity_30min
    │
    ▼
Databricks SQL Warehouse (serverless, auto-resumes)
    │
    ▼
Tool result fed back to the LLM
    │
    ▼
Natural-language answer with citations
```

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

App opens at `http://localhost:8501`. First query may take ~30 seconds while
the Databricks Serverless warehouse warms up.

## Deploy to Streamlit Community Cloud

1. Push this repo to GitHub (already done if you're reading this).
2. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with GitHub.
3. Click **New app**:
   - Repository: `demonjd2026-afk/gridsense`
   - Branch: `main`
   - Main file path: `streamlit_app/app.py`
4. In **Advanced settings → Secrets**, paste the contents of
   your `.streamlit/secrets.toml` (the real one, not the example).
5. Deploy. URL will be something like
   `https://gridsense-<hash>.streamlit.app`.

The free tier sleeps after ~7 days of inactivity and wakes on first hit
(takes ~10 seconds). Good enough for portfolio demo URLs.

## Example questions

- "What's the cleanest EU country right now?"
- "Why is Germany so dirty right now?"
- "Which UK region is cleanest?"
- "How has France's carbon trended over the last 24 hours?"
- "When should I run my UK batch job for lowest carbon?"

The agent should call exactly one tool per question, except when a question
combines two angles (e.g., "compare DE vs FR" might call get_country_fuel_mix twice).

## Why these design choices

**Why Streamlit Cloud, not Azure Container Apps?**
Free hosting, deploys from GitHub directly, zero infra burden. The agent
already runs on Azure (OpenAI) and queries Azure (Databricks); the *UI*
hosting cost is incidental.

**Why GPT-4.1-mini, not GPT-4o-mini?**
Newer model (April 2025), cheaper per token, better tool-calling
accuracy for simple SQL-backed tools. gpt-4o-mini's `2024-07-18`
version was deprecated in Azure in March 2026.

**Why 5 tools, not 1 generic SQL tool?**
Bounded tool surface reduces hallucination and SQL-injection risk.
Each tool is a hand-written query with parameter validation;
the LLM picks which to call, not what SQL to write.

**Why does this exist alongside the dashboards?**
Dashboards answer "show me the data." The agent answers "tell me what
the data means" — a different cognitive mode. Both are valid; both are
in the portfolio.

## What this does NOT do (deliberately)

- No ML forecasts (Phase 8 not yet shipped — agent extends to use forecasts
  when that fact lands)
- No write operations to the lakehouse (read-only by design)
- No history beyond the current session (no persistent memory store)
- No multi-region routing (single Azure OpenAI deployment is fine for portfolio)
