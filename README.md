# GridSense

> Carbon-Aware Energy Grid Intelligence Lakehouse on Azure

[![Terraform Apply](https://github.com/demonjd2026-afk/gridsense/actions/workflows/terraform-apply.yml/badge.svg)](https://github.com/demonjd2026-afk/gridsense/actions/workflows/terraform-apply.yml) [![Bundle Deploy](https://github.com/demonjd2026-afk/gridsense/actions/workflows/databricks-bundle-deploy.yml/badge.svg)](https://github.com/demonjd2026-afk/gridsense/actions/workflows/databricks-bundle-deploy.yml)

A near-real-time data lakehouse that ingests electricity grid telemetry from 30+ European sources, computes live carbon intensity, forecasts the next 24 hours of grid cleanliness using machine learning, and generates carbon-aware workload scheduling recommendations through a GenAI agent.

**Live demo:** [gridsense-carbon.streamlit.app](https://gridsense-carbon.streamlit.app) — ask the GenAI agent live questions about EU/UK grid carbon intensity.

![GridSense end-to-end architecture: streaming ingestion → Databricks lakehouse → LightGBM ML pipeline → live GenAI agent](docs/architecture-diagrams/gridsense-architecture.png)



### Try asking the live agent

The agent answers questions about live EU and UK grid carbon intensity. Just click any example in the sidebar, or type your own.

**Current grid state:**
- *What's the cleanest EU country right now?*
- *Why is Germany so dirty right now?*
- *Which UK region is cleanest?*
- *Show me Spain's energy mix*

**24-hour trends:**
- *How has France's carbon trended over the past 24 hours?*
- *Is Germany getting cleaner today?*

**Carbon-aware scheduling:**
- *When should I run my UK batch job for lowest carbon?*
- *What's the cleanest 30-minute window in the UK?*

**ML-powered forecasting (Phase 8):**
- *Will Germany be cleaner tomorrow than today?*
- *What's the forecast for France?*
- *Should I run my batch job in Germany tomorrow or France?*
- *What does the model predict for tomorrow's grid?*

**Multi-tool orchestration (most impressive demos):**
- *Compare DE and FR right now and tomorrow*
- *Which country has the most nuclear production?*
- *Find the cleanest place to run my workload across UK regions and EU forecasts*

The agent will honestly decline questions outside its scope (electricity prices, weather, countries not in the dataset), and the *"Show data source used"* expander under every answer reveals the exact SQL tools and parameters it called.

## Stack
- **Historical backfill (Phase 8.A/B/C):** Lambda-architecture split — live ingestion runs as a streaming pipeline (producer → Event Hubs → Bronze) while historical data flows through a one-shot batch path (API → Bronze direct, skipping Event Hubs). Three backfills (UK Carbon Intensity, ENTSO-E generation, Open-Meteo weather) populated 3 years of history without disrupting live streams. Source-tagged envelopes (`*-backfill`) preserve the audit trail through Silver MERGE.
- **GenAI agent layer (Phase 9):** Streamlit Community Cloud + Azure OpenAI (`gpt-4.1-mini` in `swedencentral`) + OpenAI tool calling over 5 hand-written SQL tools against the Gold facts. Live at [gridsense-carbon.streamlit.app](https://gridsense-carbon.streamlit.app).

**Streaming ingestion** · **Medallion architecture** · **Spark Structured Streaming** · **MLflow forecasting** · **GenAI briefing agent** · **Databricks AI/BI dashboards**

## Architecture

See the **[Live status diagram](#live-status-as-built)** below for the as-built data flow.

- **Compute:** Azure Databricks (serverless job compute)
- **Streaming ingest:** Spark Structured Streaming (Kafka surface of Event Hubs)
- **Storage:** ADLS Gen2 + Delta Lake (bronze/silver/gold/quarantine schemas)
- **Governance:** Unity Catalog
- **Producers:** Python on Azure Container Apps (one per data source)
- **Secrets:** Azure Key Vault, surfaced via UAMI to Container Apps and via KV-backed secret scope to Databricks
- **IaC:** Terraform + Databricks Asset Bundles
- **CI/CD:** GitHub Actions with OIDC federation (no client secrets); Terraform + Databricks Asset Bundles deploy on push to main
- **Monitoring:** Deferred (see [runbook](docs/runbook.md) for rationale); would reach for Application Insights for the producers and Databricks-native lakehouse monitoring if implemented

## Project status

🚧 Under active construction. Following the 12-phase implementation guide.

| Phase | Status |
|---|---|
| 1. Repository & local env | ✅ Done |
| 2. Azure foundation (Terraform) | ✅ Done |
| 3. Databricks workspace config | ✅ Done |
| 4. Data producers (Container Apps) | ✅ Done (3 producers live) |
| 5. Bronze layer streaming | ✅ Done (3 tables, hourly ingest) |
| 6. Silver layer (cleansing + joins) | ✅ Done (5 tables incl. grid_state 3-way join) |
| 7. Gold layer (star schema) | ✅ Done — 4 dims + 4 facts; `fact_grid_hourly` is the Phase 8 ML training table, `fact_carbon_forecast` materializes ML predictions |
| 8. ML forecasting (MLflow) | ✅ Done — LightGBM forecast model trained on 3yr history, live in agent ([docs/PHASE8.md](docs/PHASE8.md)) |
| 9. GenAI agent | ✅ Done ([Live demo](https://gridsense-carbon.streamlit.app)) |
| 10. Dashboards (Databricks AI/BI) | ✅ Done — 3 dashboards, see [docs/PHASE10.md](docs/PHASE10.md) |
| 11. CI/CD (GitHub Actions) | ✅ Done — Terraform + Bundle deploy via OIDC federation, no client secrets ([docs/PHASE11.md](docs/PHASE11.md)) |
| 12. Monitoring & observability | 🟡 Deferred — deliberate scope decision, see [runbook](docs/runbook.md) for rationale |

## Live status (as-built)

Three producers publish to Azure Event Hubs; thirteen Databricks jobs cascade hourly (Bronze ingest → Silver parse+join → Gold star schema) into Delta tables in Unity Catalog. The Gold layer is a two-fact star schema: a fuel-mix fact at hourly grain across 6 countries, and a carbon-intensity fact at 30-min grain across 18 UK regions. All resources provisioned via Terraform, all Databricks code deployed via Asset Bundles.

```mermaid
flowchart LR
    A1[UK Carbon Intensity API] --> P1[Azure Container App<br/><i>ca-carbon-intensity-dev</i><br/>5 min poll]
    A2[Open-Meteo API] --> P2[Azure Container App<br/><i>ca-open-meteo-dev</i><br/>15 min poll]
    A3[ENTSO-E API] --> P3[Azure Container App<br/><i>ca-entsoe-dev</i><br/>1 hr poll]
    P1 --> EH[(Azure Event Hubs<br/><i>evh-gridsense-dev</i><br/>3 topics)]
    P2 --> EH
    P3 --> EH
    EH --> B[Bronze layer<br/>3 streaming Delta tables<br/><i>Databricks + Unity Catalog</i>]
    B --> S[Silver layer<br/>5 cleaned + joined Delta tables]
    S --> G[Gold star schema<br/>4 dims + 4 facts<br/>✅ Phase 7]
    G --> ML[LightGBM Forecast Model<br/><i>Unity Catalog Model Registry</i><br/>✅ Phase 8]
    ML --> FCAST[Carbon Forecast Fact<br/><i>gold.fact_carbon_forecast</i><br/>845 predictions]
    G --> AGENT[GenAI Agent<br/><i>Streamlit + Azure OpenAI</i><br/>6 tools<br/>✅ Phase 9]
    FCAST --> AGENT
    G --> PBI[3 AI/BI Dashboards<br/><i>Databricks-native</i><br/>✅ Phase 10]
    AGENT --> URL[Live demo<br/><i>gridsense-carbon.streamlit.app</i>]

    classDef done fill:#1f6f43,stroke:#2ecc71,color:#fff
    classDef ml fill:#4a4dff,stroke:#7e80ff,color:#fff
    class P1,P2,P3,EH,B,S,G,PBI,AGENT,URL done
    class ML,FCAST ml
```

### What's running right now

| Component | Cadence | Volume |
|---|---|---|
| `ca-carbon-intensity-dev` Container App | 5 min poll | ~5,184 msg/day |
| `ca-open-meteo-dev` Container App | 15 min poll | ~576 msg/day |
| `ca-entsoe-dev` Container App | 1 hr poll | ~120 msg/day |
| `bronze_carbon_intensity` Databricks Job | Hourly (cron `0 5 * * * ?`) | reads from `carbon-intensity` topic |
| `bronze_open_meteo` Databricks Job | Hourly (cron `0 10 * * * ?`) | reads from `open-meteo` topic |
| `bronze_entsoe` Databricks Job | Hourly (cron `0 15 * * * ?`) | reads from `entsoe` topic |
| `silver_carbon_intensity` Databricks Job | Hourly at :25 | parse + dedup + MERGE into `silver.carbon_intensity` |
| `silver_open_meteo` Databricks Job | Hourly at :30 | parse + dedup + MERGE into `silver.weather` |
| `silver_entsoe` Databricks Job | Hourly at :35 | parse + dedup + MERGE into `silver.generation` |
| `silver_country_dim` Databricks Job | Hourly at :40 | static 6-row country to capital mapping |
| `silver_grid_state` Databricks Job | Hourly at :45 | 4-way join into `silver.grid_state` (the interview-worthy artifact) |
| `gold_dim_country` Databricks Job | Hourly at :50 | static dim with EIC + timezone offsets |
| `gold_dim_fuel_type` Databricks Job | Hourly at :52 | unified fuel taxonomy + IPCC AR5 lifecycle carbon |
| `gold_dim_time` Databricks Job | Hourly at :55 | 17,521-row hourly dim (2026-2028 UTC) |
| `gold_fact_generation_fuel_hourly` Databricks Job | Hourly at :57 | star-schema fact: country x hour x fuel |
| `gold_dim_uk_region` Databricks Job | Hourly at :53 | static dim: 14 UK DNO regions + 4 national rollups |
| `gold_fact_carbon_intensity_30min` Databricks Job | Hourly at :59 | star-schema fact: UK region x 30-min interval (forecast + actual) |

### Architectural decisions worth flagging

- **Source-named topics, not domain-named.** `open-meteo` topic for the open-meteo producer (not `weather`). Schema and data domain live in the event envelope, not the topic name.
- **Producer-side: managed identity all the way.** Container Apps authenticate to Event Hubs via UAMI + OAuth bearer (no connection strings, no SAS keys, no secrets).
- **Consumer-side: Service Principal workaround.** Databricks Spark Kafka client does not natively support managed identity auth as of late 2025; SP + Key Vault is the documented Microsoft pattern. Switch to UC Service Credentials when DBR 16.1+ goes GA.
- **Secret-management via Azure Key Vault.** Single source of truth: ENTSO-E API token, Databricks SP credentials. Surfaced into Container Apps via the `secrets` block in Terraform and into Databricks via a KV-backed secret scope.
- **Shared Python package between producers.** `producers/_common/` is installed editable into each producer image at build time; carries the OAuth handler and event envelope code so producer-specific files stay small.
- **MERGE-with-dedup, not append-only, in Silver.** Producers publish each natural key many times (forecast then actual, retries, TSO corrections). Silver dedupes the source DataFrame via `ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY ingested_at DESC)` before MERGE; this both fixes Delta's `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` error and gives latest-wins semantics.
- **Unified fuel taxonomy across two upstream sources.** `gold.dim_fuel_type` maps ENTSO-E PsrType codes (B01–B25) and UK Carbon Intensity plain labels (`nuclear`, `solar`, `wind`...) to a single `fuel_key`, with `is_renewable`, `is_low_carbon`, and IPCC AR5 lifecycle `typical_gco2_per_kwh`. Downstream "renewable share for FR last hour" becomes one star join, not a `CASE WHEN` ladder.
- **Two facts at different grains, not one merged fact.** `fact_generation_fuel_hourly` (country × hour × fuel) and `fact_carbon_intensity_30min` (UK region × 30-min) answer complementary questions: lifecycle CO₂ from typical fuel-mix averages vs. live measured grid intensity. Merging them into one OBT would force a grain compromise; keeping them separate lets each be queried at its natural grain and joined when needed.

## Data sources

| Source | Type | Cadence | Purpose |
|---|---|---|---|
| UK Carbon Intensity API | REST, no key | 5 min poll | Live carbon intensity (forecast + actual) for 14 UK DNO regions |
| Open-Meteo Weather | REST, no key | 15 min poll | Wind speed, solar irradiance, temperature, cloud cover for 6 EU cities |
| ENTSO-E Transparency | REST, token (KV-backed) | 1 hr poll | Actual generation per production type for 6 EU bidding zones |
| OpenStreetMap power | Static extract | Once (Phase 7) | Power plant locations |

## Outcomes targeted

| Metric | Target |
|---|---|
| End-to-end latency (event → gold) | < 60 sec (p95) |
| Data quality (DLT expectations pass) | > 99% |
| Forecast accuracy (24h carbon intensity) | MAPE < 15% |
| GenAI briefing freshness | Daily, by 06:00 UTC |
| Monthly Azure cost | < ₹10,000 (~ $120) |

## Repo layout

```
gridsense/
├── .github/workflows/   # CI/CD pipelines
├── infra/               # Terraform (foundation, eventhubs, databricks, container-apps)
│   ├── envs/            # Per-environment composition (dev, staging, prod)
│   └── modules/         # Reusable Terraform modules
├── producers/           # Python ingestion services (one per data source)
├── databricks/          # Databricks Asset Bundle (notebooks, jobs, DLT pipelines, ML, GenAI)
├── powerbi/             # Semantic model + .pbix report
├── scripts/             # Operational scripts (deploy, seed catalog, etc.)
├── Makefile             # Daily-driver commands
└── README.md            # This file
```

## Daily commands

```bash
make help              # show all available targets
make fmt               # format Terraform + Python
make lint              # run all linters
make test              # run unit tests
make deploy-dev        # deploy infra + Databricks bundle to dev
make destroy-dev       # tear down dev (stops cost accrual)
make providers-check   # verify Azure resource providers are registered
make azure-whoami      # show current Azure CLI account
```

## Documentation

- [Architecture deep-dive](docs/architecture.md) — implementation log with per-phase design decisions, issues, and resolutions
- [Operational runbook](docs/runbook.md) — day-to-day operations, FinOps cost tables, debugging, and known gotchas from production

## License

Personal portfolio project. Not for redistribution.
