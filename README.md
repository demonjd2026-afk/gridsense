# GridSense

> Carbon-Aware Energy Grid Intelligence Lakehouse on Azure

A near-real-time data lakehouse that ingests electricity grid telemetry from 30+ European sources, computes live carbon intensity, forecasts the next 24 hours of grid cleanliness using machine learning, and generates carbon-aware workload scheduling recommendations through a GenAI agent.

## Stack

**Streaming ingestion** · **Medallion architecture** · **Delta Live Tables** · **MLflow forecasting** · **GenAI briefing agent** · **Power BI on Fabric DirectLake**

## Architecture

```
SOURCES          INGESTION         LAKEHOUSE          SERVE
UK Carbon API → Event Hubs    →  Bronze (raw)     →  Fabric SQL / DirectLake
ENTSO-E API     Container Apps   Silver (clean)      Power BI live map
Open-Meteo      ADF static refs  Gold (star + ML)    GenAI briefings
```

- **Compute:** Azure Databricks (Premium, job clusters only)
- **Storage:** ADLS Gen2 + Delta Lake (bronze/silver/gold)
- **Governance:** Unity Catalog
- **IaC:** Terraform + Databricks Asset Bundles
- **CI/CD:** GitHub Actions with OIDC federation
- **Monitoring:** Azure Monitor + Log Analytics

## Project status

🚧 Under active construction. Following the 12-phase implementation guide.

| Phase | Status |
|---|---|
| 1. Repository & local env | 🟡 In progress |
| 2. Azure foundation (Terraform) | ⚪ Not started |
| 3. Databricks workspace config | ⚪ Not started |
| 4. Data producers (Container Apps) | ⚪ Not started |
| 5. Bronze layer streaming | ⚪ Not started |
| 6. Silver layer (DLT) | ⚪ Not started |
| 7. Gold layer (star schema) | ⚪ Not started |
| 8. ML forecasting (MLflow) | ⚪ Not started |
| 9. GenAI briefing agent | ⚪ Not started |
| 10. Power BI on Fabric DirectLake | ⚪ Not started |
| 11. CI/CD (GitHub Actions) | ⚪ Not started |
| 12. Monitoring & observability | ⚪ Not started |

## Data sources

| Source | Type | Cadence | Purpose |
|---|---|---|---|
| UK Carbon Intensity API | REST, no key | 5 min poll | Live carbon intensity, 48h forecast |
| ENTSO-E Transparency | REST, token | 15 min poll | Generation by fuel type, 30+ countries |
| Open-Meteo Weather | REST, no key | Hourly | Wind speed, solar irradiance (forecast features) |
| OpenStreetMap power | Static extract | Once | Power plant locations |

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

- [Architecture deep-dive](docs/architecture.md) — *(coming in Phase 2)*
- [Operational runbook](docs/runbook.md) — *(coming in Phase 12)*

## License

Personal portfolio project. Not for redistribution.
