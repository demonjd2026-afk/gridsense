# GridSense Architecture

> A near-real-time data lakehouse on Azure that ingests European electricity-grid telemetry, computes carbon intensity, and produces analytical artifacts ready for ML forecasting and GenAI briefings.

This document is the implementation log: what was built, why those choices, what broke, and how it was resolved. It complements the [README](../README.md) (which is the entry point) and the commit log (which is the change history).

---

## 1. System overview

Three Python producers running on Azure Container Apps poll three independent electricity-grid APIs and publish events to Azure Event Hubs. From there, eleven Databricks jobs cascade hourly through a medallion architecture (Bronze → Silver → Gold) into Delta Lake tables governed by Unity Catalog.

```mermaid
flowchart LR
    A1[UK Carbon Intensity API] --> P1[ca-carbon-intensity-dev]
    A2[Open-Meteo API] --> P2[ca-open-meteo-dev]
    A3[ENTSO-E API] --> P3[ca-entsoe-dev]
    P1 --> EH[(Event Hubs<br/>3 topics)]
    P2 --> EH
    P3 --> EH
    EH --> B[Bronze<br/>3 streaming tables]
    B --> S[Silver<br/>5 cleaned + joined tables]
    S --> G[Gold<br/>star schema:<br/>3 dims + 1 fact]
    G --> CONS[ML / GenAI / Power BI<br/>future phases]

    classDef done fill:#1f6f43,stroke:#2ecc71,color:#fff
    classDef wip fill:#5c3317,stroke:#f39c12,color:#fff
    class P1,P2,P3,EH,B,S done
    class G,CONS wip
```

Cadences and ownership:

| Layer | Components | Schedule |
|---|---|---|
| Producers | 3 Container Apps | Continuous (5-min, 15-min, 1-hr polls) |
| Bronze | 3 Databricks streaming jobs | Hourly at :05, :10, :15 |
| Silver per-source | 3 Databricks jobs | Hourly at :25, :30, :35 |
| Silver dim + join | 2 Databricks jobs | Hourly at :40, :45 |
| Gold dims + fact | 4 Databricks jobs | Hourly at :50, :52, :55, :57 |

Total: 3 producers + 11 Databricks jobs running 24/7, fully provisioned via Terraform and Databricks Asset Bundles.

---

## 2. Design principles

The choices below are not platitudes — each one resolved a real tension during build.

### 2.1 Medallion architecture with hard boundaries
- **Bronze:** raw envelopes, append-only, never edited. If a downstream layer is wrong, Bronze is the source of truth to rebuild from.
- **Silver:** parsed, typed, deduplicated, validated. The first layer where data is "trustable" for analytics.
- **Gold:** star schema. Analytics-ready. Surrogate keys, denormalized for query performance, lifecycle-carbon enrichment.

Each layer reads only from the layer immediately below it. No Bronze → Gold leaps.

### 2.2 Source-named topics, not domain-named
Event Hubs topic names match the producer (`open-meteo`, `entsoe`, `carbon-intensity`), not the data domain (`weather`, `generation`, `carbon`). Multiple weather providers? Each gets its own topic. The schema and data domain live in the event envelope, not the topic name.

### 2.3 Managed identity for producers, Service Principal for consumers
- **Producer-side:** Azure Container Apps use a user-assigned managed identity (UAMI) that has `Azure Event Hubs Data Sender` on the namespace. Zero secrets in the producer code.
- **Consumer-side:** Databricks Spark Kafka client does not natively support managed-identity auth (as of late 2025). The Microsoft-documented workaround is a Service Principal with client secret stored in Azure Key Vault, surfaced into Databricks via a KV-backed secret scope. This is documented as a known migration target when Unity Catalog Service Credentials for Kafka go GA.

### 2.4 Centralized secret management via Azure Key Vault
One Key Vault holds every secret: ENTSO-E API token, Databricks SP credentials. Producers reach KV via UAMI; Databricks reaches it via a KV-backed secret scope. No connection strings, no SAS keys, no `.env` files in the repo.

### 2.5 Explicit schemas everywhere
- `from_json` with declared `StructType` schemas, not inference. Catches upstream drift early, keeps Spark plans stable.
- Explicit DataFrame aliases on every join. Spark Connect (serverless) rejects ambiguous column references that classic Spark would tolerate.
- Explicit timestamp formats. Each of the three APIs emits timestamps differently; default parsers silently produce NULLs on edge cases.

### 2.6 MERGE-with-dedup, not append-only, in Silver
Producers publish each natural key multiple times (forecast → actual republished, retries, TSO corrections). Bronze is append-only and accumulates these duplicates. Silver dedupes the source DataFrame via a `ROW_NUMBER()` window function over `(natural_key, ingested_at DESC)` keeping `rn=1` *before* the MERGE. Without this, Delta raises `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`. With this, MERGE gives latest-wins semantics for free.

### 2.7 Quarantine, not crash, on bad data
Malformed envelopes never break the pipeline. Each Silver job validates required fields and writes invalid rows to `quarantine.<source>` with a `reject_reason` column. The quarantine schema is an immutable audit log: each rejection instance gets its own row even if the same payload is rejected twice. Valid rows continue into `silver.<source>`.

### 2.8 Asset Bundles for Databricks, Terraform for infrastructure
Two IaC tools, clear boundaries:
- **Terraform** owns Azure resources: resource group, storage, Event Hubs, Container Apps, Databricks workspace, Key Vault, role assignments.
- **Asset Bundles** own Databricks artifacts: notebooks, jobs, schedules.

This avoids the "everything in Terraform" trap (Databricks resources are slow to manage there) and the "everything in Asset Bundles" trap (no support for Azure-level resources).

---

## 3. Per-phase implementation log

Phases 2 and 3 (Azure foundation, Databricks workspace setup) are documented in their respective commit messages on `main` and are not duplicated here. The log below covers phases 4–7 in depth.

### Phase 4 — Producers (Container Apps)

**What was built**

Three Python services, one per data source, packaged as Docker images and deployed to Azure Container Apps:
- `producers/carbon-intensity/` — polls UK Carbon Intensity API (no auth), 14 DNO regions, 5-minute cadence
- `producers/open-meteo/` — polls Open-Meteo (no auth), 6 EU cities, 15-minute cadence
- `producers/entsoe/` — polls ENTSO-E Transparency Platform (token auth), 6 EU bidding zones, 1-hour cadence

Each producer:
1. Auths to Event Hubs via UAMI + OAuth bearer token
2. Polls its upstream API
3. Wraps each event in a versioned envelope (`event_id`, `source`, `source_version`, `ingested_at`, `event_time`, `region`, `payload`, `checksum`)
4. Publishes to its source-named topic

Shared concerns live in `producers/_common/` (auth handler, envelope construction), installed editable into each image at build time.

**Key design decisions**

- *Source-named topics, not domain-named.* See §2.2. Reified during this phase when an early draft had `weather` as a topic name; renamed to `open-meteo` to align with the producer.
- *Envelope versioning baked into v1.* `source_version` is on every event from day one. The cost of adding it now is zero; the cost of adding it later is a schema migration across millions of events.
- *Terraform `for_each` over a producers map.* Each new producer is a new map entry; no copy-paste of Terraform resources. Phase 4.H (Open-Meteo) and 4.I (ENTSO-E) were each ~30 lines of additions to the map, not 200 lines of new resources.

**Issues hit and resolutions**

1. **Open-Meteo producer always emitted UTC midnight.** Caught much later during Phase 6.A development when `silver.weather` showed only 12 rows total across 6 cities and 24 hours. Root cause: `first_hour_snapshot()` took `times[0]` from the API response, which under `forecast_days=1` is today's UTC midnight regardless of poll time. Fix: pick the index whose timestamp matches the current UTC hour, with fallback to the latest index ≤ now. Rebuilt as image v2, rolled out via Terraform tag bump. Test updated for new latest-index-wins semantics.

2. **ENTSO-E averaging vs summing.** The A75/A16 query returns 15-minute resolution (PT15M) points within hourly TimeSeries. Initial code summed all points within a TimeSeries, producing 4× the true value. Fix: average within TimeSeries.

3. **ENTSO-E PsrType codelist was outdated.** Missing B21-B25. Producer was dropping those fuel types silently. Fix: extended PsrType codelist to all 28 entries (B01-B25 + A03-A05).

4. **Promoted Container Apps from imperative to declarative.** Initial deployment used `az containerapp create` (imperative). Promoted to Terraform `for_each` over producers map with `terraform import` to absorb the existing carbon-intensity app without recreation.

### Phase 5 — Bronze layer (streaming ingestion)

**What was built**

Three Spark Structured Streaming jobs reading from Event Hubs Kafka surface, writing to Delta tables partitioned by `event_date`, with checkpoints on ADLS for exactly-once semantics:
- `bronze.carbon_intensity` ← topic `carbon-intensity`
- `bronze.open_meteo` ← topic `open-meteo`
- `bronze.entsoe` ← topic `entsoe`

Each Bronze job is `availableNow=True` triggered (batch-on-schedule, not continuous streaming), runs hourly via Databricks Asset Bundle job schedules.

**Key design decisions**

- *availableNow trigger.* For hourly cadence with cheap recovery on failure, batch-on-schedule beats continuous streaming. Lower cost, simpler reasoning, same end-state.
- *Envelope preserved as JSON string.* Bronze does not parse the envelope. The envelope_json column carries the raw producer output untouched. Parsing happens in Silver. Bronze stays append-only and reversible.
- *Partition by event_date.* One file per day per topic. Cheap pruning for date-range queries; small enough to avoid the small-files problem.

**Issues hit and resolutions**

1. **Databricks does not natively support cluster managed-identity Kafka auth.** First job run failed with `Failed to create new KafkaAdminClient`. Researched: as of late 2025, Spark Kafka client in Databricks does not pick up the cluster's managed identity for OAuth. The Microsoft-documented workaround is a Service Principal with client secret in Key Vault, surfaced via KV-backed Databricks secret scope. Implemented:
   - New Azure AD application + Service Principal via Terraform `azuread` provider
   - SP granted `Azure Event Hubs Data Receiver` on the namespace
   - 1-year client secret + client_id + tenant_id stored as 3 KV secrets
   - Manual one-off (documented as runbook): granted the first-party AzureDatabricks SP `Key Vault Secrets User` on the vault so the workspace can read the KV-backed scope
   - KV-backed scope `gridsense-kv` created via Databricks UI
   - common.py rewritten to fetch SP credentials via `dbutils.secrets.get` and build the OAUTHBEARER JAAS config
   - Documented as a migration target: when Unity Catalog Service Credentials for Kafka go GA (DBR 16.1+), the SP can be retired in favor of the existing Databricks Access Connector UAMI.

2. **common.py missing `# Databricks notebook source` header.** `%run ./common` from the per-topic notebooks resolved to "notebook not found" because Databricks did not register common.py as a notebook without the header. Fix: added the magic header line at the top.

### Phase 6 — Silver layer (cleansing + joins)

**What was built**

Phase 6 split into two sub-phases:

*6.A — Three per-source Silver tables:*
- `silver.carbon_intensity` ← parse + dedup + validate `bronze.carbon_intensity`
- `silver.weather` ← parse + dedup + validate `bronze.open_meteo`
- `silver.generation` ← parse + dedup + validate `bronze.entsoe`

Each reads Bronze in batch (not streaming — MERGE semantics need batch), parses the envelope JSON with explicit `StructType` schemas, validates required fields, then either MERGEs into Silver on the natural key OR appends to `quarantine.<source>` with a `reject_reason`. Same dedup-before-MERGE primitive across all three, defined once in `databricks/src/silver/common.py`.

*6.B — Three-way join:*
- `silver.country_dim` — static 6-row mapping (country_code → capital_city), used as the bridge between weather (city grain) and generation (country grain)
- `silver.grid_state` — the integration artifact. 4-way LEFT JOIN from `silver.generation` (the spine, since it covers all 6 countries continuously) to `silver.country_dim`, then to `silver.weather` (via capital_city), then to a GB-only aggregated subquery from `silver.carbon_intensity`. Output at `(country_code, hour_utc)` grain.

**Key design decisions**

- *generation as the spine, LEFT joins for the rest.* Generation is the only source covering all 6 EU countries hourly. Weather is nullable when the producer has not yet published the matching hour. UK carbon intensity is GB-only by design — the regional API has no continental equivalent. Modeling these as LEFT joins makes the gaps explicit and analytically queryable rather than silently dropping rows.
- *30-min → hourly aggregation for UK carbon intensity.* UK Carbon Intensity publishes 30-minute settlement periods. We aggregate to hourly by `date_trunc('hour', period_start) + AVG(intensity_forecast)` so the grain matches generation's hourly grain.
- *Quarantine schema as immutable audit log.* Bad rows go into `quarantine.<source>` with `reject_reason` and `reject_at_ts`. No MERGE, no overwrite — each rejection is its own row. Bronze stays untouched; Silver stays clean; rejections stay auditable.

**Issues hit and resolutions**

1. **Delta MERGE failed with `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` on second run.** First run created the table via append (table didn't exist), wrote 7038 rows (the full Bronze count with all duplicates). Second run hit MERGE, found multiple source rows per natural key, refused to proceed. Root cause: Bronze accumulates all retries/republishes; many rows per natural key. Fix: dedupe the source DataFrame *before* MERGE via `ROW_NUMBER() OVER (PARTITION BY natural_key ORDER BY ingested_at DESC)` keeping `rn=1`. Now in `common.py.merge_into_silver`, used by all Silver jobs and reused unchanged in Gold. Latest-wins semantics fall out for free.

2. **Open-Meteo producer always emitted UTC midnight (cross-referenced from Phase 4).** First detected here when `silver.weather` had only 12 distinct rows across 24 hours × 6 cities. Confirmed via raw Bronze SQL showing each `(city, payload.time)` pair had ~85 rows for one single timestamp per UTC day. Fix is in the producer (Phase 4); Silver was correct.

3. **Serverless compute rejects `.cache()` / `.persist()`.** Initial `common.py` cached the parsed DataFrame to avoid re-computing it for `count()`, `valid.count()`, `invalid.count()`. Serverless threw `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless compute`. Fix: removed the `.cache()` call. On the data volumes involved (~7000 rows), re-computation is cheap and Spark caches blocks transparently.

4. **Spark's default `to_timestamp` rejects timestamps without seconds.** Open-Meteo emits `2026-05-14T00:00` (no seconds, no timezone). The producer's documented intent is UTC. Initial Silver appended `+00:00` to make `2026-05-14T00:00+00:00`, which Spark *still* rejected because the parser requires `:ss`. Fix: append `:00+00:00` so the resulting string `2026-05-14T00:00:00+00:00` is parser-friendly.

5. **Carbon Intensity API has no `intensity.actual` for the regional endpoint.** Schema initially declared `intensity_actual` as a nullable column expecting eventual population. Quickly discovered the regional API publishes forecasts only; actuals only come from the *national* `/intensity` endpoint. Decision: keep `intensity_actual` as a documented-nullable column rather than dropping it. Removing now means a backwards-incompatible schema change if we add the national endpoint in Phase 7+.

6. **Spark Connect rejects ambiguous column references that classic Spark tolerates.** First `silver.grid_state` run failed with `AMBIGUOUS_REFERENCE` on `capital_city`, which exists on both `country_dim` and `weather` after the join. Classic Spark would have warned; Spark Connect refused. Fix: alias every source DataFrame (`g`, `d`, `w`, `c`) and prefix every column reference throughout the join. This pattern is now reused in every multi-source join across Silver and Gold.

### Phase 7 — Gold layer (star schema)

**What was built (7.A)**

Phase 7.A shipped three dimensions and one fact table:

- `gold.dim_country` (6 rows, static) — extends `silver.country_dim` with EIC bidding-zone codes, ISO alpha-3 codes, and winter/summer timezone offsets
- `gold.dim_fuel_type` (29 rows, static) — unifies ENTSO-E PsrType codes (`B01`-`B25`) and UK Carbon Intensity plain labels (`nuclear`, `solar`, `wind`...) into a single `fuel_key`, with `is_renewable`, `is_low_carbon`, and IPCC AR5 lifecycle `typical_gco2_per_kwh`
- `gold.dim_time` (17,521 rows, generated) — hourly grain, 2026-01-01 to 2028-01-01 UTC. Carries year/quarter/month/week/day/hour numerics, `is_weekend`, `is_business_hour_uk`, `is_daytime_approx`, and a `yyyyMMddHH` integer `time_key` for cheap fact joins
- `gold.fact_generation_fuel_hourly` (2,070 rows at Phase 7.B close, growing hourly) — explodes `silver.generation.generation_mix` into one row per (country, hour, fuel). Joins to all 3 dims via surrogate keys. Computes `estimated_gco2_per_hour = value_mw × typical_gco2_per_kwh`

**What was built (7.B)**

Phase 7.B was originally scoped as `fact_grid_hourly` (a wide fact joining weather and generation). On opening 7.B, a Silver-table inspection showed that `silver.carbon_intensity` had been quietly accumulating ~2,000 rich rows across 18 UK regions at 30-min grain — measurably more demoable than a wide fact built on still-sparse weather. The scope pivoted accordingly. `fact_grid_hourly` slides to a later phase when weather density justifies it.

7.B shipped one dimension and one fact:

- `gold.dim_uk_region` (18 rows, static) — 14 GB DNO regions plus 4 national rollups (England, Scotland, Wales, GB) from the UK Carbon Intensity API. Carries `region_type` ("DNO" vs "national") and approximate lat/lon for Phase 10 Power BI maps. Rolls up to `dim_country` via `country_code = "GB"`.
- `gold.fact_carbon_intensity_30min` (2,070 rows at close) — UK carbon intensity at 30-min settlement-period grain. One row per (region_id, period_start). Carries `intensity_forecast` (always populated), `intensity_actual` (nullable; backfilled in a later phase), `source_type` discriminator, pre-computed `forecast_minus_actual` for Phase 8 model evaluation, and a denormalized `generation_mix` array.

**Pending (7.C — later phase)**

`gold.fact_grid_hourly` — wide fact from `silver.grid_state`, one row per (country, hour). Deferred until Open-Meteo weather data accumulates ~7 days of density across all 6 countries.

**Key design decisions**

- *Unified fuel taxonomy in `dim_fuel_type` is the payoff dim.* Without it, "what was the renewable share for FR last hour?" is a `CASE WHEN` ladder over PsrType strings. With it, that question is a one-line `WHERE is_renewable = true` filter. This is the dim that makes the rest of the schema worth its weight.
- *IPCC AR5 lifecycle carbon estimates as a static column.* `typical_gco2_per_kwh` is a hand-curated column sourced from IPCC AR5 WG3 Annex III (lifecycle medians: coal 820, gas 490, nuclear 12, solar 48, wind 11 gCO2-eq/kWh). It complements live grid carbon intensity from `silver.carbon_intensity` (which captures actual mix at a moment) by enabling "what's the typical emission profile of this country's fuel mix" without needing live carbon data for non-UK countries.
- *Two facts at different grains, not one merged fact.* `fact_generation_fuel_hourly` (country × hour × fuel, lifecycle CO₂) and `fact_carbon_intensity_30min` (UK region × 30-min, measured CO₂) answer complementary questions. Merging them into one OBT would force a grain compromise; keeping them separate lets each be queried at its natural grain and joined when needed.
- *Separate `dim_uk_region` rather than extending `dim_country`.* UK carbon intensity publishes per-DNO-region (London at 18:00 can be 189 gCO₂/kWh while South Scotland is 0) — a granularity `dim_country` cannot represent without breaking the one-row-per-country invariant. `dim_uk_region` joins to `dim_country` via `country_code` so country-level rollup still works in a single hop.
- *Inner join to dimensions, not left.* If an upstream PsrType or `region_id` is missing from its dim, that's a real data-quality issue we want to FAIL LOUDLY. A LEFT join with NULL would silently produce broken rows.
- *`source_type` discriminator instead of separate forecast/actual tables.* The UK API emits each period twice — first as forecast (`intensity_actual = null`), later as actual. One fact with a `source_type` column ("forecast" | "actual") and a pre-computed `forecast_minus_actual` measure means Phase 8's ML model can evaluate forecast accuracy without a join. Two tables would have required UNION-or-join logic everywhere downstream.
- *`time_key` deliberately hourly even for the 30-min fact.* `fact_carbon_intensity_30min` has two rows per `time_key`, distinguished by a `half_hour` column (0 or 30). `GROUP BY time_key` becomes the natural hourly rollup for BI; the denormalized `period_start` timestamp on the fact handles direct 30-min queries without a time-dim join. Avoids maintaining a parallel `dim_time_15min` that would duplicate 95% of `dim_time`'s columns.
- *`dim_time` sized to actual data window + 20-month forward.* Not 10 years. Larger ranges send a misleading signal about data availability. 2026-01-01 to 2028-01-01 honestly reflects: a few months before producers came online, 20 months of forward horizon for ML.

**Issues hit and resolutions**

Phase 7.A shipped with zero bugs. Phase 7.B shipped with zero bugs in the build itself. The patterns established in Silver (alias DataFrames, dedup before MERGE, common.py helpers) composed cleanly into Gold without modification. This is the strongest evidence in the project that the architecture is paying off.

Two judgment calls during 7.B worth recording, neither a bug:

1. *Initial ad-hoc table creation in the wrong workspace abstraction.* The 7.B dim and fact were first created via the SQL editor with unqualified table names (`gold.dim_uk_region` instead of `${catalog}.gold.dim_uk_region`). They happened to land in the right catalog because `dbw_gridsense_dev` is the workspace default, but this is fragile across targets. Resolution: dropped the SQL-editor tables, rebuilt via the Asset Bundle with the standard catalog-widget pattern matching every other Gold notebook. The bundle is now the single source of truth.
2. *Mid-phase scope pivot.* Started 7.B intending to build `fact_grid_hourly`; switched to `fact_carbon_intensity_30min` after `SHOW TABLES IN silver` revealed `silver.carbon_intensity` had richer ready-to-use data. The pivot cost ~30 minutes of design re-thinking and produced a substantially stronger demo (the UK regional intensity spread — South Wales at 365 gCO₂/kWh while Scotland sits at 0 — is the single most striking query result in the project so far). Recorded here because portfolio-honesty matters: shipping the right thing late is better than shipping the planned thing on schedule.

---
## 4. Cross-cutting concerns

### 4.1 Authentication architecture

Two distinct identity paths because of a Databricks limitation discussed in section 2.3:

| Identity | Used by | Granted | How |
|---|---|---|---|
| uami-gridsense-producers-dev (UAMI) | 3 Container Apps | Azure Event Hubs Data Sender on namespace | Producer code calls DefaultAzureCredential; IMDS returns a token |
| sp-gridsense-databricks-eh-dev (SP) | Databricks Spark Kafka client | Azure Event Hubs Data Receiver on namespace | Spark JAAS config reads SP credentials from dbutils.secrets.get |
| Databricks Access Connector UAMI | Unity Catalog to ADLS | Storage Blob Data Contributor on storage account | Unity Catalog external locations |
| AzureDatabricks first-party SP | Databricks workspace to Key Vault | Key Vault Secrets User on KV | KV-backed secret scope |

The producer-side UAMI also has Key Vault Secrets User for fetching the ENTSO-E API token. One identity, two responsibilities.

### 4.2 Secret management

One Key Vault (kv-gridsense-dev-dx0kcg) holds every secret. Two consumption paths:

| Consumer | Mechanism |
|---|---|
| Container Apps | Terraform-declared secrets block on Container App, mounted as environment variables via UAMI |
| Databricks notebooks | KV-backed secret scope gridsense-kv; dbutils.secrets.get(scope, key) |

Secrets in KV today:

- entsoe-api-token for the ENTSO-E producer
- databricks-eh-sp-client-id, databricks-eh-sp-secret, databricks-eh-sp-tenant-id for the Databricks Kafka auth path

Zero secrets in the repo. Zero connection strings. Zero .env files in version control.

### 4.3 Job orchestration cadence

All Databricks jobs run hourly on staggered cron schedules so each layer reads fresh upstream data. Bronze runs at :05/:10/:15, Silver per-source at :25/:30/:35, Silver dim+join at :40/:45, and Gold dims+fact at :50/:52/:55/:57.

Stagger of 20 min between Bronze and Silver, 5 min between Silver and Gold. Empirically, every job finishes in under 90 seconds, so there is substantial slack between layers; a long Bronze run cannot delay its corresponding Silver run.

If any job fails, the next hourly attempt picks up where the previous left off (MERGE-on-natural-key is idempotent). No backfill orchestration needed; the design absorbs single-run failures naturally.

### 4.4 Schema design conventions

- Natural keys explicit on every table. Bronze: implicit per-event. Silver: (region, period_start) / (city, time_utc) / (country, period_start). Gold facts: (country, hour_utc, fuel_key). Documented in each notebook's docstring.
- Surrogate keys in Gold facts only. time_key is yyyyMMddHH as BIGINT for cheap fact joins. country_key and fuel_key are string natural keys reused from the dims; surrogate integer keys would add no value for 6-row and 29-row dims.
- Timestamps stored as TIMESTAMP in UTC. Producer-side emits ISO strings with explicit offsets where the upstream API supports it. Silver casts to TIMESTAMP with explicit format strings, never default parsers.
- Generation mix as ARRAY of STRUCT in Silver, exploded in Gold. Silver preserves the upstream nested shape for traceability; Gold flattens it for analytical queries.

### 4.5 IaC discipline

| Layer | Tool | Lives in |
|---|---|---|
| Azure resources (RG, storage, EH, KV, ACA, Databricks workspace, role assignments, Azure AD apps) | Terraform | infra/envs/dev/, infra/modules/ |
| Databricks artifacts (notebooks, jobs, schedules, secret scope hookups) | Databricks Asset Bundles | databricks/databricks.yml, databricks/resources/, databricks/src/ |
| Manual one-offs (KV-backed secret scope creation, AzureDatabricks first-party SP role grant) | Documented runbook | _local_notes.md |

Pre-commit hooks (ruff, ruff-format, terraform fmt, terraform validate, secret detection, large file detection, YAML/JSON/TOML lint) run on every commit. The workflow is git add then git commit (hooks may auto-fix) then git add -A then git commit again. This catches drift before it lands on origin.

---

## 5. Known gaps and next phases

### 5.1 Current architectural gaps

- **GB has no rows in silver.generation.** ENTSO-E consistently returns no_data for GB on the A75/A16 endpoint we query. This means silver.grid_state and gold.fact_generation_fuel_hourly only cover 5 of the 6 countries (DE/ES/FR/IT/NL). UK Carbon Intensity gives GB-specific carbon data but no GB generation mix, so the asymmetry shows up as uk_carbon_intensity_forecast IS NOT NULL AND country_code = 'GB' being an empty set in silver.grid_state. Workarounds for a future phase: use an alternative GB generation source (BMRS), or model GB as a separate carbon-only fact.

- **Weather sparseness pending v2 producer accumulation.** The Open-Meteo v2 producer (current-hour fix) deployed mid-session. As of writing, silver.weather has roughly 18 distinct hours; will reach full density within 24 hours.

- **Quarantine tables are zero rows today.** Every Silver job validates and routes invalid rows to quarantine.source, but upstream data has been clean enough that nothing has been rejected yet. This is good (data is clean) but means the quarantine path is unexercised. A defensive test injecting a known-bad event would prove the path end-to-end; not yet done.

- **intensity_actual always NULL in silver.carbon_intensity.** The regional Carbon Intensity API only publishes forecasts. Actuals come from the national /intensity endpoint, which the producer does not currently poll. Decision logged in section 3 Phase 6.

### 5.2 Next phases

| Phase | Status | Notes |
|---|---|---|
| 7.B fact_grid_hourly | Pending | Waiting for 24 hrs of dense weather data |
| 8 ML forecasting | Pending | Needs 1-2 weeks of historical data minimum |
| 9 GenAI briefing agent | Pending | Needs Gold complete (7.B) |
| 10 Power BI on Fabric DirectLake | Pending | Needs Gold complete + Fabric capacity provisioning |
| 11 CI/CD GitHub Actions | Pending | OIDC federation + terraform plan/apply from Actions |
| 12 Monitoring & observability | Pending | Azure Monitor alerts + Log Analytics queries on producers and jobs |

---

*Last updated: 2026-05-15 after Phase 7.B. See commit log for change history.*
