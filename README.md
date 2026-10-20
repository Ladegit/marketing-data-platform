# Marketing Data Platform with Databricks, dbt and Elementary

This project brings Facebook Ads and Snapchat Ads data into Databricks and uses dbt to prepare it for reporting. The two platforms have different data structures, so the project cleans and standardizes their data before combining it into shared marketing models.

The main model is `fct_marketing_campaign_daily`. It brings campaign performance into one place so analysts can compare spend, impressions and clicks without querying each ad platform separately.

The project also includes data quality tests, dbt documentation, Elementary monitoring, and GitHub Actions workflows for validating changes and deploying models.

> **Project note:** The README describes the intended setup and the implementation details captured in this repository. Before presenting it as production-ready, make sure each environment, permission, workflow and monitoring step has been run and verified in your Databricks workspace and GitHub Actions.

---

## Table of contents

1. [Business problem](#1-business-problem)
2. [Architecture](#2-architecture)
3. [Repository structure](#3-repository-structure)
4. [Source data and profiling](#4-source-data-and-profiling)
5. [Data model, grain and keys](#5-data-model-grain-and-keys)
6. [Standardizing the two platforms](#6-standardizing-the-two-platforms)
7. [Metric definitions](#7-metric-definitions)
8. [Materialization choices](#8-materialization-choices)
9. [Data quality tests](#9-data-quality-tests)
10. [Documentation and lineage](#10-documentation-and-lineage)
11. [Monitoring with Elementary](#11-monitoring-with-elementary)
12. [Environments](#12-environments)
13. [Production authentication with a service principal](#13-production-authentication-with-a-service-principal)
14. [Unity Catalog permissions](#14-unity-catalog-permissions)
15. [CI/CD](#15-cicd)
16. [Setup and running the project](#16-setup-and-running-the-project)
17. [Example business questions](#17-example-business-questions)
18. [Using the data for analytics and AI](#18-using-the-data-for-analytics-and-ai)
19. [Key engineering decisions](#19-key-engineering-decisions)
20. [Limitations and next steps](#20-limitations-and-next-steps)
21. [Implementation checklist](#21-implementation-checklist)

---

## 1. Business problem

| Problem today | How this platform solves it |
|---|---|
| Marketers log in to each ad platform and combine data in Excel | Both platforms are loaded into Databricks and combined automatically by dbt |
| Facebook and Snapchat name and structure data differently | Staging models rename/cast every field to one shared contract (`spend`, `impressions`, `clicks`, `campaign_name`, …) |
| Data only accessible to Marketing | Published in Unity Catalog with role-based grants for analysts, BI and service accounts |
| Data not reusable | Layered models (staging → intermediate → marts) with dimensions + facts that any team can join to |
| No governance, lineage or quality monitoring | dbt tests, dbt docs (static HTML + lineage DAG) and Elementary observability report |

---

## 2. Architecture

```
┌────────────────┐   ┌────────────────┐
│  Facebook Ads  │   │  Snapchat Ads  │          SOURCES
│  (5 CSV files) │   │  (4 CSV files) │
└───────┬────────┘   └───────┬────────┘
        └──────────┬─────────┘
                   ▼
   ┌─────────────────────────────────┐
   │ Unity Catalog Volume            │          LANDING
   │ /Volumes/bronze_<env>/landing/  │  (files kept as-is, auditable)
   └───────────────┬─────────────────┘
                   ▼  ingestion notebook (COPY INTO / read_files)
   ┌─────────────────────────────────┐
   │ BRONZE  bronze_<env>.marketing_raw │      RAW (Delta, all STRING,
   │ facebook_* / snapchat_* tables  │        + _loaded_at, _source_file)
   └───────────────┬─────────────────┘
                   ▼  dbt (sources.yml)
   ┌─────────────────────────────────┐
   │ STAGING   stg_facebook__*       │  views — rename, cast, clean,
   │           stg_snapchat__*       │          dedupe, normalize
   └───────────────┬─────────────────┘
                   ▼
   ┌─────────────────────────────────┐
   │ INTERMEDIATE int_*              │  ephemeral/view — enrich with
   │ int_facebook__ad_daily_enriched │  campaign/ad-group/account,
   │ int_snapchat__ad_daily_enriched │  then UNION ALL the platforms
   │ int_marketing__ad_daily_unioned │
   └───────────────┬─────────────────┘
                   ▼
   ┌─────────────────────────────────┐
   │ MARTS (gold)                    │  incremental Delta facts +
   │ fct_ad_performance_daily        │  table dimensions
   │ fct_marketing_campaign_daily ★  │  ★ = the main campaign-level model
   │ dim_campaign · dim_ad · dim_platform
   └──────┬──────────┬─────────┬─────┘
          ▼          ▼         ▼
      Analysts    BI tool   AI agent        CONSUMPTION
      (SQL)     (dashboards) (Claude / Genie, natural language)

   ── Cross-cutting ────────────────────────────────────────────
   dbt tests · Elementary (elementary schema + HTML report)
   dbt docs (static HTML + lineage) · GitHub Actions CI/CD
   Service Principal (prod) · Unity Catalog grants
```

The architecture keeps a strict separation between **raw data** (bronze, never modified), **transformation logic** (dbt staging/intermediate), **analytical models** (marts) and the **consumption layer** (grants + documented marts only).

---

## 3. Repository structure

```
marketing-data-platform/
├── README.md
├── .github/workflows/
│   ├── ci.yml                     # PR validation (CI env)
│   └── deploy_prod.yml            # merge to main → prod run (Service Principal)
├── databricks/
│   ├── 00_setup_catalogs.sql      # catalogs, schemas, volume
│   ├── 01_unity_catalog_grants.sql
│   └── 02_ingest_raw_ads.py       # loads CSVs → bronze Delta tables
├── data/                          # the 9 provided CSV files (sample data)
├── marketing_dbt/
│   ├── dbt_project.yml
│   ├── packages.yml               # dbt_utils, dbt_expectations, elementary
│   ├── profiles.yml               # env-var driven, NO secrets
│   ├── macros/
│   │   ├── generate_schema_name.sql
│   │   └── safe_divide.sql
│   ├── models/
│   │   ├── staging/
│   │   │   ├── facebook/   _facebook__sources.yml, _facebook__models.yml,
│   │   │   │               stg_facebook__ad_stats.sql, stg_facebook__ads.sql,
│   │   │   │               stg_facebook__campaigns.sql, stg_facebook__ad_groups.sql,
│   │   │   │               stg_facebook__accounts.sql
│   │   │   └── snapchat/   _snapchat__sources.yml, _snapchat__models.yml,
│   │   │                   stg_snapchat__ad_stats.sql, stg_snapchat__ads.sql,
│   │   │                   stg_snapchat__ad_groups.sql, stg_snapchat__campaigns.sql
│   │   ├── intermediate/  int_facebook__ad_daily_enriched.sql,
│   │   │                  int_snapchat__ad_daily_enriched.sql,
│   │   │                  int_marketing__ad_daily_unioned.sql, _int__models.yml
│   │   └── marts/marketing/
│   │                      fct_ad_performance_daily.sql,
│   │                      fct_marketing_campaign_daily.sql,
│   │                      dim_campaign.sql, dim_ad.sql, dim_platform.sql,
│   │                      _marketing__models.yml, _marketing__docs.md
│   ├── seeds/platform_mapping.csv
│   ├── tests/                     # singular tests
│   │   ├── assert_fct_reconciles_to_sources.sql
│   │   └── assert_clicks_not_greater_than_impressions.sql
│   └── analyses/business_questions.sql
└── docs/                          # generated dbt docs (static index.html) + screenshots
```

---

## 4. Source data and profiling

I checked the nine CSV files before building the models. The findings below influenced how I handled the joins, keys, missing values and reporting grain.

### Facebook Ads (5 files, numeric `BIGINT` ids)

| Raw file → bronze table | Rows | Role | Key |
|---|---:|---|---|
| `ads_stats.csv` → `facebook_ads_stats` | 92,300 | **Daily performance** (spend, impressions, clicks) | `ad_id` + `ad_date` (verified unique) |
| `new_ads_de_randomized.csv` → `facebook_ads` | 98,926 | Ad dimension → links ad to campaign, ad group, account; has `ad_status` | `ad_id` |
| `new_campaign_de_randomized.csv` → `facebook_campaigns` | 4,162 | Campaign names | `campaign_id` |
| `new_adgroup_de_randomized.csv` → `facebook_ad_groups` | 13,433 | Ad group (ad set) names | `ad_group_id` |
| `new_acccount_de_randomized.csv` → `facebook_accounts` | 16 | Account name and **currency** | `account_id` |

### Snapchat Ads (4 files, `UUID` string ids)

| Raw file → bronze table | Rows | Role | Key |
|---|---:|---|---|
| `snapchat_ads_stats.csv` → `snapchat_ads_stats` | 2,809 | **Daily performance by country** | `ad_detail_id` + `ad_date` + `ad_country` (verified unique) |
| `snapchat_ads_ad_history_new_ad_names.csv` → `snapchat_ad_history` | 2,513 | Ad → ad group | `ad_id` |
| `snapchat_ads_adgroup_history_new_ad_group_names.csv` → `snapchat_ad_group_history` | 267 | Ad group → campaign (**history / SCD**) | `_surrogate_key` |
| `snapchat_ads_campaign_history_new_campaign_names.csv` → `snapchat_campaign_history` | 109 | Campaign → account, campaign names | `campaign_id` |

### Data-quality findings that shaped the design

| # | Finding | Impact / how it is handled |
|---|---|---|
| 1 | **Different hierarchies.** Facebook stats carry `ad_id` + `account_id` and link to campaigns via the ads table. Snapchat stats carry only `ad_detail_id`; campaign is reached via ad → ad group → campaign. | Each platform gets its own enrichment model (`int_*__ad_daily_enriched`) that resolves the hierarchy before the union. |
| 2 | **Different id types** (Facebook 13–18 digit integers, Snapchat UUIDs). | All ids cast to `STRING` in staging so they can share one column; global uniqueness is guaranteed by prefixing keys with the platform. |
| 3 | **Literal `'null'` strings** in Facebook `ad_impressions` (1,474 rows) and `ad_clicks` (24,337 rows). | `try_cast(nullif(lower(trim(col)),'null') as bigint)`; then `coalesce(…, 0)` in marts (a missing count means *nothing reported*, not unknown spend). A raw `_is_metric_missing` flag is preserved for auditing. |
| 4 | **Different grain.** Snapchat stats are **ad × day × country** — `ad_detail_id + ad_date` has 1,757 "duplicates" that are actually different countries (`de`, `ch`, `at`, `us`). | Not deduplicated (they are real rows). Grain of the ad-level fact is **ad × day × country**; Facebook rows get `country_code = 'unknown'`. |
| 5 | **Mixed currencies.** Facebook accounts are 14 × EUR and 2 × USD — **USD accounts carry ~73% of Facebook spend** (6.87M USD vs 2.56M EUR). Snapchat is all EUR. | `currency_code` is part of every fact's grain, a dimension in every aggregate, and documented as *"never sum spend across currencies"*. FX conversion is a documented next step (see §20). |
| 6 | **SCD history in Snapchat ad groups** — 3 ad groups appear twice with different `ad_group_updated_at`. | `qualify row_number() over (partition by ad_group_id order by ad_group_updated_at desc) = 1` → latest version only. Tested with `unique` on `ad_group_id`. |
| 7 | **Orphans** — 12% of Snapchat ad-history rows point to ad groups not in the history file. All 63 ads that actually have stats resolve fully (ad → ad group → campaign). | Left joins + `coalesce(campaign_name,'unknown')`, and a `relationships` test (severity `warn`) so new orphans are visible in Elementary without blocking prod. |
| 8 | **No exact duplicates** in either stats file; no negative values; clicks never exceed impressions. | Still tested — the tests protect against *future* loads. |
| 9 | **Non-overlapping date ranges** — Facebook: 2025-01-01 → 2025-12-31; Snapchat: 2024-01-01 → 2024-12-02. | Documented in the model description so analysts/AI agents don't misread "Snapchat spent 0 in 2025" as a real comparison. |
| 10 | Snapchat has a `data_src` column; Facebook has none. | Replaced by a standard `platform` column (`'facebook'`, `'snapchat'`) driven by the `platform_mapping` seed. |

---

## 5. Data model, grain and keys

### Layer overview

| Layer | Catalog.schema (prod) | Purpose | Who reads it |
|---|---|---|---|
| Bronze / raw | `bronze_production.marketing_raw` | Exact copy of source files + load metadata | dbt only |
| Staging | `marketing_production.staging` | 1:1 with sources, cleaned, renamed, typed | dbt only |
| Intermediate | `marketing_production.intermediate` (mostly ephemeral) | Reusable joins & platform union | dbt only |
| Marts | `marketing_production.marts` | Facts & dimensions — the data product | Analysts, BI, AI agents |
| Observability | `marketing_production.elementary` | Test results, run results, anomalies | Data engineering |

### Model catalogue, grain and keys

| Model | Grain (one row = …) | Primary key | Materialization |
|---|---|---|---|
| `stg_facebook__ad_stats` | one Facebook ad on one day | `facebook_ad_stat_id` = hash(ad_id, ad_date) | view |
| `stg_facebook__ads` / `__campaigns` / `__ad_groups` / `__accounts` | one Facebook ad / campaign / ad group / account | natural id | view |
| `stg_snapchat__ad_stats` | one Snapchat ad on one day in one country | `snapchat_ad_stat_id` = hash(ad_id, ad_date, country) | view |
| `stg_snapchat__ad_groups` | one Snapchat ad group (latest version) | `ad_group_id` | view |
| `stg_snapchat__ads` / `__campaigns` | one Snapchat ad / campaign | natural id | view |
| `int_facebook__ad_daily_enriched` | one Facebook ad-day with campaign/ad group/account attributes | `ad_performance_id` | ephemeral |
| `int_snapchat__ad_daily_enriched` | one Snapchat ad-day-country with hierarchy attributes | `ad_performance_id` | ephemeral |
| `int_marketing__ad_daily_unioned` | one ad-day-country on any platform (common contract) | `ad_performance_id` | view |
| **`fct_ad_performance_daily`** | **one ad, on one platform, on one day, in one country** | `ad_performance_id` = hash(platform, ad_id, date_day, country_code) | **incremental (merge)** |
| **`fct_marketing_campaign_daily` ★** | **one campaign, on one platform, on one day, in one currency** | `campaign_performance_id` = hash(platform, campaign_id, date_day, currency_code) | **incremental (merge)** |
| `dim_campaign` | one campaign per platform | `campaign_key` = hash(platform, campaign_id) | table |
| `dim_ad` | one ad per platform (with `ad_status`, ad group, campaign) | `ad_key` = hash(platform, ad_id) | table |
| `dim_platform` | one advertising platform | `platform` | table (from seed) |

### Why the project uses both facts, dimensions and a wide model

- **`fct_ad_performance_daily`** is the atomic, lowest-grain fact — reusable by any team (creative analysis, country analysis).
- **`fct_marketing_campaign_daily`** is the **campaign-level marketing model**: a deliberately *wide*, pre-joined, campaign-day table with names and derived metrics. This is the main table for common reporting questions, with campaign names and metrics already joined.
- **Dimensions** (`dim_campaign`, `dim_ad`, `dim_platform`) hold descriptive attributes once, so attributes like `ad_status` don't get duplicated thousands of times in the atomic fact, and so analysts can list campaigns that had *no* spend.

A star schema keeps the model reusable, but users need to understand the joins. A wide campaign-level table is easier to query for common reporting questions. This project keeps both approaches: the detailed fact and dimensions for flexible analysis, plus a wider table for everyday reporting.

### Unified model contract — `fct_marketing_campaign_daily`

| Column | Type | Description |
|---|---|---|
| `campaign_performance_id` | string | Surrogate PK: hash(platform, campaign_id, date_day, currency_code) |
| `date_day` | date | Day the performance occurred (platform reporting date) |
| `date_month` | date | First day of the month — convenience for monthly reporting |
| `platform` | string | `facebook` or `snapchat` (lower-case, accepted values) |
| `platform_display_name` | string | `Facebook`, `Snapchat` — for dashboards |
| `account_id` | string | Platform-native ad account id |
| `account_name` | string | Account name (Facebook) / `unknown` where the platform doesn't supply it |
| `campaign_id` | string | Platform-native campaign id (cast to string) |
| `campaign_name` | string | Campaign name at latest known version |
| `currency_code` | string | ISO currency of `spend` (`EUR`, `USD`) |
| `spend` | decimal(18,4) | Amount spent, in `currency_code` |
| `impressions` | bigint | Number of times ads were shown (0 if not reported) |
| `clicks` | bigint | Number of clicks (0 if not reported) |
| `ctr` | double | clicks / impressions — NULL when impressions = 0 |
| `cpc` | decimal | spend / clicks — NULL when clicks = 0 |
| `cpm` | decimal | spend / impressions × 1000 — NULL when impressions = 0 |
| `active_ads_count` | int | Distinct ads with any delivery that day |
| `_loaded_at` | timestamp | When the row was (re)built by dbt — used for incremental logic and freshness |

---

## 6. Standardizing the two platforms

| Unified column | Facebook source | Snapchat source |
|---|---|---|
| `platform` | constant `'facebook'` | `data_src` → `'snapchat'` |
| `ad_id` | `ads_stats.ad_id` (BIGINT → STRING) | `snapchat_ads_stats.ad_detail_id` (UUID) |
| `date_day` | `ad_date` | `ad_date` |
| `country_code` | not provided → `'unknown'` | `upper(ad_country)` → `DE`, `CH`, `AT`, `US` |
| `campaign_id` | via `ads.campaign_id` | via ad_history → ad_group_history → `campaign_id` |
| `campaign_name` | `campaigns.campaign_name` | `campaign_history.campaign_name` |
| `ad_group_name` | `ad_groups.ad_group_name` | `ad_group_history.ad_group_name` (latest) |
| `account_id` / `account_name` | `ads_stats.account_id` / `accounts.account_name` | `campaign_history.account_id` / `'unknown'` |
| `currency_code` | `accounts.ad_currency_code` | `ad_currency_code` |
| `spend` | `ad_spend` | `ad_spend` (rounded to 4 dp) |
| `impressions` | `ad_impressions` (`'null'` → NULL) | `ad_impressions` |
| `clicks` | `ad_clicks` (`'null'` → NULL) | `ad_clicks` |

The models use `snake_case` column names. Dates use the `date_` prefix, timestamps end in `_at`, and metrics use business-friendly names such as `spend`.

Example staging model (`stg_facebook__ad_stats.sql`):

```sql
with source as (
    select * from {{ source('facebook_ads', 'facebook_ads_stats') }}
),
renamed as (
    select
        cast(ad_id as string)                                          as ad_id,
        cast(account_id as string)                                     as account_id,
        cast(ad_date as date)                                          as date_day,
        cast(ad_spend as decimal(18,4))                                as spend,
        try_cast(nullif(lower(trim(ad_impressions)), 'null') as bigint) as impressions,
        try_cast(nullif(lower(trim(ad_clicks)), 'null') as bigint)      as clicks,
        _loaded_at
    from source
),
deduplicated as (
    select * from renamed
    qualify row_number() over (partition by ad_id, date_day order by _loaded_at desc) = 1
)
select
    {{ dbt_utils.generate_surrogate_key(['ad_id', 'date_day']) }} as facebook_ad_stat_id,
    *,
    (impressions is null or clicks is null) as is_metric_missing
from deduplicated
```

Dedup by `_loaded_at` makes the pipeline idempotent: re-loading a file (or receiving a restated day from the platform) never double counts.

---

## 7. Metric definitions

Documented once in `_marketing__docs.md` (dbt `{% docs %}` blocks) and reused on every column:

| Metric | Definition | Zero handling |
|---|---|---|
| **Spend** | Amount charged by the platform, in the account currency | Never summed across currencies |
| **Impressions** | Times an ad was displayed | Missing → 0 |
| **Clicks** | Clicks reported by the platform | Missing → 0 |
| **CTR** | `clicks / impressions` | NULL if impressions = 0 |
| **CPC** | `spend / clicks` | NULL if clicks = 0 |
| **CPM** | `spend / impressions × 1000` | NULL if impressions = 0 |

Implemented through a `safe_divide` macro:

```sql
{% macro safe_divide(numerator, denominator) %}
    case when {{ denominator }} is null or {{ denominator }} = 0 then null
         else {{ numerator }} / {{ denominator }} end
{% endmacro %}
```

**When aggregating:** recalculate CTR, CPC and CPM from the aggregated totals. For example, use `sum(spend) / nullif(sum(clicks), 0)` for CPC rather than taking the average of row-level CPC values.

---

## 8. Materialization choices

| Model(s) | Materialization | Why |
|---|---|---|
| `stg_*` | **view** | Light renames/casts over bronze; always fresh; no storage cost; only read by dbt during builds. |
| `int_facebook__…`, `int_snapchat__…enriched` | **ephemeral** | Pure join logic used by exactly one downstream model; compiled as a CTE, so no extra objects in the catalog for users to stumble into. |
| `int_marketing__ad_daily_unioned` | **view** | Reused by two facts and the dims; a view keeps a debuggable object without duplicating storage. |
| `fct_ad_performance_daily` | **incremental**, `incremental_strategy='merge'`, `unique_key='ad_performance_id'`, Delta, `partition_by` not used (small), `liquid_clustered_by=['date_day','platform']` | Largest table (~95k rows and growing daily). Only new/changed days need processing. Merge (not append) because ad platforms **restate the last few days** (late conversions/attribution). |
| `fct_marketing_campaign_daily` | **incremental (merge)** | Same refresh pattern; the most queried table, so it must be a physical, clustered Delta table for fast BI/AI queries. |
| `dim_*` | **table** | Small (≤ 100k rows), fully rebuilt each run — simpler and safer than tracking changes. |
| `platform_mapping` | **seed** | Static reference data owned in Git. |

Incremental logic with a lookback window for late-arriving/restated data:

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='campaign_performance_id',
    on_schema_change='append_new_columns',
    liquid_clustered_by=['date_day', 'platform']
) }}
...
{% if is_incremental() %}
  where date_day >= (select date_sub(max(date_day), {{ var('lookback_days', 3) }}) from {{ this }})
{% endif %}
```

`dbt build --full-refresh` remains available for backfills. Considered and rejected: **materializing every model as a table** (wasteful compute/storage for pass-through staging) and **incremental staging** (adds complexity at the layer where full logic changes are most frequent).

---

## 9. Data quality tests

The tests focus on issues that could affect reporting, such as duplicate rows, missing relationships, unexpected values and totals that no longer match the source data.

| Test | Where | Protects against | Severity |
|---|---|---|---|
| `unique` + `not_null` on every PK / surrogate key | all models | Double counting spend after a re-load or a broken join (fan-out) | error |
| `dbt_utils.unique_combination_of_columns` `[ad_id, date_day, country_code]` | `stg_snapchat__ad_stats` | Confirms the true Snapchat grain (the country dimension) | error |
| `not_null` on `date_day`, `platform`, `campaign_id`, `currency_code`, `spend` | facts | Rows that can't be attributed or summed | error |
| `accepted_values` `platform in ('facebook','snapchat')` | facts, dims | Typos / new platform added without modelling | error |
| `accepted_values` `currency_code in ('EUR','USD')` | facts | A new currency silently mixed into totals | error |
| `accepted_values` `ad_status in ('ACTIVE','PAUSED','ARCHIVED','DELETED')` | `dim_ad` | Unknown status values | warn |
| `dbt_utils.accepted_range` `spend >= 0`, `impressions >= 0`, `clicks >= 0` | staging & facts | Negative metrics from bad exports | error |
| `dbt_utils.accepted_range` `ctr between 0 and 1` | facts | Impossible ratios | error |
| Singular `assert_clicks_not_greater_than_impressions` | facts | Corrupt/misaligned columns | warn |
| `relationships` fact.campaign_key → `dim_campaign`, fact.ad_key → `dim_ad` | facts | Orphaned performance with no campaign | error |
| `relationships` Snapchat ad → ad group → campaign | staging | Missing hierarchy (12% of ad history already orphaned) | warn |
| Singular `assert_fct_reconciles_to_sources` | marts | **Total spend/impressions/clicks in the mart = bronze totals** per platform & currency. Catches any join that drops or duplicates rows. | error |
| `dbt_expectations.expect_row_values_to_have_recent_data` / `source freshness` | sources | Pipeline silently stopping | warn |
| `dbt_expectations.expect_column_values_to_be_of_type` | facts | Schema drift in the source | warn |
| Elementary `volume_anomalies`, `column_anomalies (spend, clicks)` | facts | Sudden drops/spikes (e.g. half a day loaded) | warn |

Tests marked `error` should fail the build when a check fails. Tests marked `warn` flag an issue for investigation without necessarily stopping the run; confirm the configured severity and workflow behaviour in the project before relying on it in production.

Reconciliation test (key idea):

```sql
-- tests/assert_fct_reconciles_to_sources.sql  → returns rows only on mismatch
with src as (
  select 'facebook' as platform, round(sum(spend),2) s from {{ ref('stg_facebook__ad_stats') }}
  union all
  select 'snapchat', round(sum(spend),2) from {{ ref('stg_snapchat__ad_stats') }}
), mart as (
  select platform, round(sum(spend),2) s from {{ ref('fct_marketing_campaign_daily') }} group by 1
)
select * from src join mart using (platform) where abs(src.s - mart.s) > 0.01
```

---

## 10. Documentation and lineage

- Every **source**, **model** and **important column** has a description in YAML; shared metric definitions use `{% docs %}` blocks so `spend`, `ctr`, `cpc`, `cpm` mean the same thing everywhere.
- Every model description starts with **"Grain: one row = …"**.
- Sources include `loader`, `freshness`, and owner `meta`; marts include `meta: {owner: data-engineering, contains_pii: false, consumer: [bi, ai_agent]}`.
- Static HTML docs are generated and committed under `/docs` and published as a CI artifact:

```bash
dbt docs generate --target prod --static     # produces target/static_index.html
```

Lineage (as shown in the dbt docs DAG):

```
facebook_ads.facebook_ads_stats ──► stg_facebook__ad_stats ─┐
facebook_ads.facebook_ads ────────► stg_facebook__ads ──────┤
facebook_ads.facebook_campaigns ──► stg_facebook__campaigns ┼─► int_facebook__ad_daily_enriched ─┐
facebook_ads.facebook_ad_groups ──► stg_facebook__ad_groups ┤                                     │
facebook_ads.facebook_accounts ───► stg_facebook__accounts ─┘                                     │
                                                                                                    ├─► int_marketing__ad_daily_unioned ─┬─► fct_ad_performance_daily
snapchat_ads.snapchat_ads_stats ──► stg_snapchat__ad_stats ─┐                                     │                                     ├─► fct_marketing_campaign_daily ★
snapchat_ads.snapchat_ad_history ─► stg_snapchat__ads ──────┼─► int_snapchat__ad_daily_enriched ─┘                                     ├─► dim_campaign
snapchat_ads.snapchat_ad_group_history ► stg_snapchat__ad_groups ┤                                                                     └─► dim_ad
snapchat_ads.snapchat_campaign_history ► stg_snapchat__campaigns ┘
seed: platform_mapping ──────────────────────────────────────────────────────────────────────────► dim_platform
```

---

## 11. Monitoring with Elementary

**Setup**

```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.1.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]
  - package: elementary-data/elementary
    version: [">=0.16.0", "<0.17.0"]
```

```yaml
# dbt_project.yml
models:
  elementary:
    +schema: elementary          # results land in <catalog>.elementary
```

```bash
dbt deps
dbt run --select elementary --target dev      # creates Elementary tables once per env
pip install "elementary-data[databricks]"
edr report --profile-target prod             # builds elementary_report.html
```

**What it gives the team**

| Requirement | Delivered by |
|---|---|
| Test results & failed tests | `elementary_test_results` table + report "Test results" tab |
| Model health | `dbt_run_results` — run status, duration, rows affected per model |
| Data anomalies | `volume_anomalies` on both facts, `column_anomalies` on `spend`/`clicks`, `freshness_anomalies` on `_loaded_at` |
| Historical test behaviour | Elementary keeps every run; report shows pass/fail trend per test over time |
| Lineage-aware triage | Report lineage highlights which downstream models a failing test affects |

The production workflow runs `edr report` (and optionally `edr monitor --slack-webhook …`) after every `dbt build`, uploading the HTML report as a workflow artifact so the team can see platform health without opening Databricks.

---

## 12. Environments

| | **CI** | **Development / Staging** | **Production** |
|---|---|---|---|
| Purpose | Validate every PR before merge | Build & test while developing | Serve trusted data to consumers |
| Trigger | Pull request → `main` | Developer runs locally / dbt Cloud IDE | Merge to `main` + daily schedule (06:00 WAT) |
| dbt target | `ci` | `dev` | `prod` |
| Raw data catalog | `bronze_development` | `bronze_development` | `bronze_production` |
| Model catalog | `ci_development` | `development` | `marketing_production` |
| Schemas | `ci_pr_<PR_NUMBER>_staging/…/marts` (isolated per PR, dropped after) | `dbt_<username>_staging/…/marts` | `staging`, `intermediate`, `marts`, `elementary` |
| Identity | Developer/CI identity (`<DEVELOPER_EMAIL>` grants) | Developer's own identity (PAT/OAuth, dev only) | **Service Principal (OAuth M2M)** — no personal token |
| Data | Sample / limited (`--vars '{lookback_days: 3}'`, `--empty` for structure checks) | Full sample data | Full data |
| Can write to prod? | No | No | Yes (only identity that can) |

Schema separation is handled by a custom `generate_schema_name` macro so that prod uses clean names (`marts`) while dev/CI get prefixed, isolated schemas:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' -%}
        {{ custom_schema_name | trim if custom_schema_name else target.schema }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim if custom_schema_name else 'default' }}
    {%- endif -%}
{%- endmacro %}
```

`profiles.yml` — all secrets come from environment variables / GitHub secrets, never Git:

```yaml
marketing_dbt:
  target: dev
  outputs:
    dev:
      type: databricks
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      catalog: development
      schema: "dbt_{{ env_var('DBT_USER', 'dev') }}"
      token: "{{ env_var('DATABRICKS_DEV_TOKEN') }}"     # personal, dev only
      threads: 4
    ci:
      type: databricks
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      catalog: ci_development
      schema: "ci_pr_{{ env_var('PR_NUMBER', 'local') }}"
      token: "{{ env_var('DATABRICKS_CI_TOKEN') }}"
      threads: 4
    prod:
      type: databricks
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      catalog: marketing_production
      schema: marts
      auth_type: oauth
      client_id: "{{ env_var('DATABRICKS_SP_CLIENT_ID') }}"         # Service Principal application id
      client_secret: "{{ env_var('DATABRICKS_SP_CLIENT_SECRET') }}" # Service Principal OAuth secret
      threads: 8
```

---

## 13. Production authentication with a service principal

The production target is configured to use a Databricks service principal rather than a developer's personal access token. This keeps deployment authentication separate from an individual developer account.

The setup described for this project is:

1. **Account console → User management → Service principals → Add**: `sp-marketing-dbt-prod`.
2. Assigned the service principal to the workspace and gave it the **Databricks SQL access** entitlement (+ `CAN USE` on the prod SQL warehouse).
3. Generated an **OAuth secret** for the service principal (machine-to-machine OAuth). Recorded the **Application ID** (`12345678-90ab-cdef-1234-567890abcdef`) and the secret.
4. Stored them as **GitHub Actions secrets** in a protected `production` environment: `DATABRICKS_SP_CLIENT_ID`, `DATABRICKS_SP_CLIENT_SECRET` (plus `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`).
5. Applied the Unity Catalog grants in §14 to the Application ID.
6. The `prod` dbt target uses `auth_type: oauth` with `client_id`/`client_secret` — verified with `dbt debug --target prod` (connection OK, identity = service principal).
7. Least privilege: the SP owns the production model catalog; humans get read-only `SELECT` on `marts`. Secrets are rotated every 90 days.

Proof in Databricks: *Query History* for prod runs shows the user as the service principal's application id, not a person.

---

## 14. Unity Catalog permissions

`databricks/01_unity_catalog_grants.sql` (replace `<SP_APPLICATION_ID>` with the service principal's application id):

```sql
-- ===== Service Principal: production =====
GRANT CREATE CATALOG ON METASTORE                 TO `12345678-90ab-cdef-1234-567890abcdef`;

-- Raw / bronze
GRANT USE CATALOG    ON CATALOG bronze_production TO `12345678-90ab-cdef-1234-567890abcdef`;
GRANT USE SCHEMA     ON CATALOG bronze_production TO `12345678-90ab-cdef-1234-567890abcdef`;
GRANT CREATE SCHEMA  ON CATALOG bronze_production TO `12345678-90ab-cdef-1234-567890abcdef`;
GRANT CREATE TABLE   ON CATALOG bronze_production TO `12345678-90ab-cdef-1234-567890abcdef`;
GRANT USE SCHEMA     ON SCHEMA  bronze_production.marketing_raw TO `12345678-90ab-cdef-1234-567890abcdef`;
GRANT SELECT         ON SCHEMA  bronze_production.marketing_raw TO `12345678-90ab-cdef-1234-567890abcdef`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME bronze_production.landing.ads_files TO `12345678-90ab-cdef-1234-567890abcdef`;

-- Transformed models (dbt writes here)
GRANT USE CATALOG, USE SCHEMA, CREATE SCHEMA, CREATE TABLE, SELECT, MODIFY
      ON CATALOG marketing_production TO `12345678-90ab-cdef-1234-567890abcdef`;

-- ===== CI / development: own user =====
GRANT USE CATALOG ON CATALOG ci_development TO `omoladebisiriyu1@gmail.com`;
GRANT USE SCHEMA  ON CATALOG ci_development TO `omoladebisiriyu1@gmail.com`;
GRANT SELECT      ON CATALOG ci_development TO `omoladebisiriyu1@gmail.com`;
GRANT CREATE SCHEMA, CREATE TABLE, MODIFY ON CATALOG ci_development TO `omoladebisiriyu1@gmail.com`;
GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG bronze_development TO `omoladebisiriyu1@gmail.com`;

-- ===== Consumers: read-only on the data product =====
GRANT USE CATALOG ON CATALOG marketing_production        TO `marketing_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA marketing_production.marts TO `marketing_analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA marketing_production.marts TO `bi_service_accounts`;
```

Principle: **only the service principal can write to production**; developers can write only to `development`/`ci_development`; consumers can only `SELECT` from `marts` (staging/intermediate are hidden from them). Grants on schemas created later are inherited because they are granted at catalog level. dbt `grants:` config in `dbt_project.yml` re-applies `select` to `marketing_analysts` on every marts build so a `CREATE OR REPLACE` never drops access.

---

## 15. CI/CD

```
Developer → feature/<ticket> branch → Pull Request → CI (GitHub Actions)
                                                        │  dbt deps
                                                        │  sqlfluff lint
                                                        │  dbt build --target ci  (models + tests)
                                                        │  dbt docs generate
                                                        │  drop ci_pr_<n>_* schemas
                                                        ▼
                                      review + green checks (branch protection)
                                                        ▼
                                              Merge to main
                                                        ▼
                         Deploy to Production (environment: production, SP OAuth)
                              dbt source freshness → dbt build --target prod
                              → dbt docs generate --static → edr report
                              (+ daily cron schedule)
```

`.github/workflows/ci.yml` (abridged):

```yaml
name: dbt CI
on:
  pull_request:
    branches: [main]
jobs:
  dbt-ci:
    runs-on: ubuntu-latest
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_HTTP_PATH: ${{ secrets.DATABRICKS_HTTP_PATH }}
      DATABRICKS_CI_TOKEN: ${{ secrets.DATABRICKS_CI_TOKEN }}
      PR_NUMBER: ${{ github.event.pull_request.number }}
    defaults: { run: { working-directory: marketing_dbt } }
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install dbt-databricks sqlfluff sqlfluff-templater-dbt
      - run: dbt deps
      - run: sqlfluff lint models --dialect databricks
      - run: dbt build --target ci --profiles-dir .
      - run: dbt docs generate --target ci --profiles-dir .
      - name: Drop PR schemas
        if: always()
        run: dbt run-operation drop_ci_schemas --target ci --profiles-dir .
```

`.github/workflows/deploy_prod.yml` (abridged):

```yaml
name: dbt Production
on:
  push:
    branches: [main]
  schedule:
    - cron: "0 5 * * *"          # 06:00 Africa/Lagos
  workflow_dispatch:
jobs:
  dbt-prod:
    runs-on: ubuntu-latest
    environment: production       # protected env, holds SP secrets, requires approval
    env:
      DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
      DATABRICKS_HTTP_PATH: ${{ secrets.DATABRICKS_HTTP_PATH }}
      DATABRICKS_SP_CLIENT_ID: ${{ secrets.DATABRICKS_SP_CLIENT_ID }}
      DATABRICKS_SP_CLIENT_SECRET: ${{ secrets.DATABRICKS_SP_CLIENT_SECRET }}
    defaults: { run: { working-directory: marketing_dbt } }
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install dbt-databricks "elementary-data[databricks]"
      - run: dbt deps
      - run: dbt debug --target prod --profiles-dir .
      - run: dbt source freshness --target prod --profiles-dir .
      - run: dbt build --target prod --profiles-dir .
      - run: dbt docs generate --target prod --static --profiles-dir .
      - run: edr report --profile-target prod --profiles-dir . --file-path elementary_report.html
        if: always()
      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: dbt-docs-and-elementary
          path: |
            marketing_dbt/target/static_index.html
            marketing_dbt/elementary_report.html
```

GitHub Actions keeps validation and deployment steps alongside the project code. Pull requests can be checked before merging, and separate schemas help prevent one development run from interfering with another. If the project grows, Slim CI using `state:modified+` and `--defer` is one possible improvement.

---

## 16. Setup and running the project

**Prerequisites:** Databricks workspace with Unity Catalog, a SQL warehouse, Python 3.11, `dbt-databricks ≥ 1.8`.

```bash
# 1. Clone
git clone https://github.com/Ladegit/marketing-data-platform.git
cd marketing-data-platform

# 2. Create catalogs, schemas, volume and grants (run in Databricks SQL editor as admin)
#    databricks/00_setup_catalogs.sql
#    databricks/01_unity_catalog_grants.sql

# 3. Upload the 9 CSVs to /Volumes/bronze_<env>/landing/ads_files/ and run the ingestion notebook
#    databricks/02_ingest_raw_ads.py   → creates bronze_<env>.marketing_raw.facebook_* / snapchat_*

# 4. Install dbt and packages
python -m venv .venv && source .venv/bin/activate
pip install dbt-databricks "elementary-data[databricks]"
cd marketing_dbt && dbt deps

# 5. Configure env vars (dev)
export DATABRICKS_HOST=... DATABRICKS_HTTP_PATH=... DATABRICKS_DEV_TOKEN=... DBT_USER=omolade
dbt debug --target dev

# 6. Build, test, document
dbt seed --target dev
dbt build --target dev
dbt docs generate --static --target dev && open target/static_index.html
edr report --profile-target dev
```

Ingestion (bronze) — loads everything as STRING so bad values like `'null'` never fail the load; typing happens in dbt where it is tested and documented:

```python
files = {
  "facebook_ads_stats":        "ads_stats.csv",
  "facebook_ads":              "new_ads_de_randomized.csv",
  "facebook_campaigns":        "new_campaign_de_randomized.csv",
  "facebook_ad_groups":        "new_adgroup_de_randomized.csv",
  "facebook_accounts":         "new_acccount_de_randomized.csv",
  "snapchat_ads_stats":        "snapchat_ads_stats.csv",
  "snapchat_ad_history":       "snapchat_ads_ad_history_new_ad_names.csv",
  "snapchat_ad_group_history": "snapchat_ads_adgroup_history_new_ad_group_names.csv",
  "snapchat_campaign_history": "snapchat_ads_campaign_history_new_campaign_names.csv",
}
catalog = dbutils.widgets.get("catalog")          # bronze_development | bronze_production
base = f"/Volumes/{catalog}/landing/ads_files"
from pyspark.sql import functions as F
for table, f in files.items():
    (spark.read.option("header", True).option("inferSchema", False).csv(f"{base}/{f}")
          .withColumn("_loaded_at", F.current_timestamp())
          .withColumn("_source_file", F.lit(f))
          .write.mode("overwrite").option("overwriteSchema", True)
          .saveAsTable(f"{catalog}.marketing_raw.{table}"))
```

---

## 17. Example business questions

The examples below query `marketing_production.marts.fct_marketing_campaign_daily`. They are based on the sample data included with the project.

**Spend, impressions, clicks and efficiency by platform and currency**

```sql
select platform, currency_code,
       sum(spend) as spend, sum(impressions) as impressions, sum(clicks) as clicks,
       sum(clicks) / nullif(sum(impressions), 0)          as ctr,
       sum(spend)  / nullif(sum(clicks), 0)               as cpc,
       sum(spend)  / nullif(sum(impressions), 0) * 1000   as cpm
from marts.fct_marketing_campaign_daily
group by 1, 2 order by 1, 2;
```

| platform | currency | spend | impressions | clicks | CTR | CPC | CPM |
|---|---|---:|---:|---:|---:|---:|---:|
| facebook | EUR | 2,562,561.24 | 295,373,593 | 1,158,683 | 0.39% | 2.21 | 8.68 |
| facebook | USD | 6,869,536.99 | 610,075,828 | 4,579,027 | 0.75% | 1.50 | 11.26 |
| snapchat | EUR | 30,529.54 | 33,395,784 | 19,012 | 0.06% | 1.61 | 0.91 |

Facebook all currencies: 905,449,421 impressions and 5,737,710 clicks (CTR 0.63%) vs Snapchat 33,395,784 impressions and 19,012 clicks (CTR 0.06%).

- **Facebook vs Snapchat spend (like-for-like EUR):** Facebook €2.56M vs Snapchat €30.5K. Facebook also spent $6.87M in USD accounts — reported separately until FX conversion is added.
- **Which platform generated more clicks/impressions?** Facebook, by a wide margin on both.
- **Note:** Facebook data covers 2025, Snapchat covers Jan–Dec 2024, so the comparison is across different periods.

**Top campaigns by impressions / clicks / spend**

```sql
select platform, campaign_name, currency_code,
       sum(impressions) impressions, sum(clicks) clicks, sum(spend) spend
from marts.fct_marketing_campaign_daily
group by 1, 2, 3 order by impressions desc limit 10;   -- or order by clicks / spend
```

- Most impressions: `usa_ios_display_ggl_awareness_20230916_interest-stack-v3_video-views` (Facebook, 84.0M) — it also has the most clicks (1.39M).
- Highest spend: `gbr_app_influencer_met_retargeting_20230728_lifetime-offer-v2_website-visits` (Facebook, 1.27M).

**Performance over time**

```sql
select date_month, platform, currency_code, sum(spend) spend, sum(clicks) clicks,
       sum(spend)/nullif(sum(clicks),0) cpc
from marts.fct_marketing_campaign_daily
group by 1,2,3 order by 1,2;
```

Facebook peaked in **January 2025** (~1.25M spend across currencies) with a second high in **November 2025** (~0.90M); Snapchat peaked in **February 2024** (€9.2K) and had no spend in April, May and October 2024.

**"How much did we spend on Facebook last month?"** → `where platform = 'facebook' and date_month = add_months(date_trunc('month', current_date()), -1)`.

---

## 18. Using the data for analytics and AI

The campaign model is also intended for use by BI tools and AI assistants such as Claude or Databricks Genie. Clear column names and documented metric rules help these tools interpret the data correctly:

- **A straightforward starting point:** `fct_marketing_campaign_daily` has readable column names and does not require joins for common campaign-level questions.
- **Column descriptions are available in Unity Catalog** through dbt's `persist_docs` configuration, giving users and connected tools context about each field.
- **Metric guidance is documented:** spend should not be added across currencies, ratio metrics should be recalculated from totals, platform values are lowercase, and the date coverage differs by platform.
- **Checks and freshness:** dbt tests check the models, Elementary provides run and test results, and `_loaded_at` records when data was loaded.
- **Access control:** the intended consumer access is read-only on the marts schema.

Example: *"Compare Facebook and Snapchat spend"* → agent groups by `platform, currency_code` and explains that Facebook has both EUR and USD accounts — exactly the behaviour the documentation instructs.

---

## 19. Key engineering decisions

| Question | Decision & reasoning |
|---|---|
| **Model structure?** | Hybrid: atomic fact + wide campaign-day fact + dimensions. Atomic fact for reuse; wide model for usability by non-technical users and AI; dims to avoid repeating attributes. |
| **Grain?** | Atomic: ad × platform × day × country (Snapchat's native grain; Facebook country = `unknown`). Unified model: campaign × platform × day × **currency** — currency is in the grain so totals are never silently mixed. |
| **Keys?** | Hash surrogate keys including `platform`, because Facebook and Snapchat ids are different types and could theoretically collide once cast to string. |
| **Why incremental?** | Facts grow daily and platforms restate recent days → `merge` on the surrogate key with a 3-day lookback is idempotent and cheap. |
| **Why views/ephemeral upstream?** | Cheap, always in sync with bronze, and don't clutter the catalog consumers browse. |
| **Why these tests?** | Each maps to a way a business answer could become wrong: double counting (unique), lost rows (reconciliation), mixed currencies (accepted_values), impossible values (ranges), broken hierarchy (relationships). |
| **Platform differences?** | Resolved per platform in staging/intermediate, then `UNION ALL` into one contract; the downstream user never sees `ad_detail_id` or `data_src`. |
| **Null handling?** | `'null'` strings → NULL in staging (auditable flag kept) → 0 for counts in marts; ratios → NULL on zero denominators (not 0, not infinity). |
| **Environments?** | Separate catalogs per environment + per-developer/per-PR schemas → no one can overwrite prod or each other. |
| **Securing prod?** | Service principal via OAuth M2M, secrets in a protected GitHub environment, only the SP can write to prod catalogs, consumers are read-only. |
| **CI/CD approach?** | GitHub Actions with branch protection: lint → build → test → docs on PR; build + observability on merge and on schedule. Simple, auditable, lives with the code. |

---

## 20. Limitations and next steps

1. **Currency conversion** — add a daily FX rates source and a `spend_eur` column so cross-currency totals become possible.
2. **Snapchat account names** are not in the sample (only account UUIDs) → `account_name = 'unknown'`; add the Snapchat account history table.
3. **Facebook country** is not in the sample → `unknown`; ingest the country breakdown if needed.
4. **SCD Type 2** — campaign/ad-group renames currently resolve to the latest name; add `dbt snapshot`s to keep full history.
5. **Automated ingestion** — replace one-off CSV loads with Fivetran/Airbyte or the platforms' APIs + Auto Loader, and schedule with Databricks Workflows.
6. **Semantic layer** — define `spend`, `ctr`, `cpc`, `cpm` as dbt Semantic Layer / Databricks metric views so every tool computes them identically.
7. **Slim CI** with `state:modified+` and `--defer` as the project grows.

---

## 21. Implementation checklist

- [x] Facebook data loaded into Databricks (`bronze_<env>.marketing_raw.facebook_*`)
- [x] Snapchat data loaded into Databricks (`bronze_<env>.marketing_raw.snapchat_*`)
- [x] Raw data accessible through Unity Catalog structures (catalog → schema → table, volume for files)
- [x] dbt connects to dev, CI and prod (`dbt debug` per target)
- [x] Staging models implemented (9)
- [x] Intermediate models implemented (3)
- [x] Unified marketing model created (`fct_marketing_campaign_daily`)
- [x] Grain clearly defined for every model
- [x] Data quality tests implemented (generic, package, singular, Elementary anomaly)
- [x] Important columns and models documented
- [x] dbt static HTML documentation generated (`/docs`, CI artifact)
- [x] Model lineage visible in dbt docs
- [x] Elementary configured with report
- [x] CI environment implemented (`ci_development`, per-PR schemas)
- [x] Development/staging environment implemented (`development`, per-user schemas)
- [x] Production environment implemented (`marketing_production`)
- [x] Production uses a Service Principal (OAuth M2M), not a PAT
- [x] Unity Catalog permissions configured
- [x] Appropriate materializations used (view, ephemeral, incremental, table, seed)
- [x] CI/CD workflow implemented (GitHub Actions)
- [x] README contains setup and architecture documentation
- [x] Final model answers the required business questions

---

*Author: Omolade Bisiriyu · Tools used: Databricks, Unity Catalog, dbt-databricks, dbt_utils, dbt_expectations and Elementary.*
