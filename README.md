# GA4 Analytics Data Platform

> Enterprise-grade analytics data platform for 80oeste — built on AWS with a Medallion architecture (Bronze → Silver → Gold). Ingests raw Google Analytics 4 event exports, transforms them through a fully automated pipeline, and exposes analytics-ready tables in Redshift Serverless for Power BI and QuickSight.

[![CI — Tests](https://github.com/80Olcalvo/ga4-analytics-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/80Olcalvo/ga4-analytics-data-platform/actions/workflows/ci.yml)
[![Deploy — Core Infra](https://github.com/80Olcalvo/ga4-analytics-data-platform/actions/workflows/deploy-core.yml/badge.svg)](https://github.com/80Olcalvo/ga4-analytics-data-platform/actions/workflows/deploy-core.yml)
[![Deploy — Pipeline](https://github.com/80Olcalvo/ga4-analytics-data-platform/actions/workflows/deploy-pipeline.yml/badge.svg)](https://github.com/80Olcalvo/ga4-analytics-data-platform/actions/workflows/deploy-pipeline.yml)

---

## Table of Contents

- [Architecture](#architecture)
- [Gold Layer — Data Mart](#gold-layer--data-mart)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [CI/CD](#cicd)
- [Running Tests](#running-tests)
- [Deployment](#deployment)
- [Connecting BI Tools](#connecting-bi-tools)
- [AWS Resources](#aws-resources)

---

## Architecture

The platform follows the **Medallion Architecture** pattern — three progressive layers of data quality:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MEDALLION ARCHITECTURE                          │
├──────────────────────┬──────────────────────┬───────────────────────────┤
│   🥉 BRONZE           │   🥈 SILVER           │   🥇 GOLD                 │
│   Raw, immutable     │   Flattened,         │   Modelled for BI         │
│                      │   partitioned        │   consumption             │
│  s3://ga4-raw/       │  s3://ga4-curated/   │  Redshift gold schema     │
└──────────────────────┴──────────────────────┴───────────────────────────┘
```

### End-to-End Pipeline

```
New .parquet in S3 (Bronze)
        │
        ▼  EventBridge trigger
Step Functions: ga4-etl-pipeline
        │
        ├─ [Step 1] Glue ETL Job: ga4-flatten-etl
        │    Flattens nested GA4 Parquet → partitioned Silver in S3
        │
        ├─ [Step 2] Glue Crawler: ga4-curated-crawler
        │    Updates Glue Data Catalog (ga4_curated database)
        │
        └─ [Step 3] Lambda: ga4-gold-refresh
             TRUNCATE + INSERT on all Gold tables in Redshift
             SNS notification on success or failure
```

### Component Diagram

```
S3 Bronze ──► Glue ETL ──► S3 Silver ──► Glue Crawler ──► Glue Catalog
                                                                  │
                                              Redshift Spectrum ◄─┘
                                                      │
                                              public.ga4_analytics (view)
                                                      │
                    ┌─────────────────────────────────┼──────────────────────┐
                    │                                 │                      │
             gold.dim_users               gold.dim_sessions      gold.fact_daily_events
             gold.fact_page_performance   gold.dim_visitor_profile
                    │
                    └──► Power BI / QuickSight
```

---

## Gold Layer — Data Mart

Five materialized tables in the `gold` schema, refreshed on every pipeline run:

| Table | Grain | Description |
|---|---|---|
| `gold.dim_users` | 1 row / user | Lifetime user profile: acquisition, device, revenue totals |
| `gold.dim_sessions` | 1 row / session | Session-level metrics: pages, engagement, conversion |
| `gold.fact_daily_events` | 1 row / day+event+geo+device+source | Aggregated daily event metrics |
| `gold.fact_page_performance` | 1 row / day+page+geo+device+source | Page-level engagement and traffic |
| `gold.dim_visitor_profile` | 1 row / visitor | **Full behavioral profile**: journey, segmentation, navigation patterns, engagement, acquisition, geography |

### dim_visitor_profile — Column Reference

| Column | Type | Description |
|---|---|---|
| `user_pseudo_id` | VARCHAR(64) | Primary key — anonymous visitor ID |
| `first_seen_date` | DATE | Date of first recorded event |
| `last_seen_date` | DATE | Date of most recent event |
| `total_sessions` | INTEGER | Count of distinct sessions |
| `total_events` | INTEGER | Total event count |
| `days_since_first_visit` | INTEGER | Days between first and last visit |
| `days_since_last_visit` | INTEGER | Days since last visit (relative to max event_date) |
| `active_days` | INTEGER | Distinct days with at least one event |
| `visitor_type` | VARCHAR(10) | `'New'` (1 session) or `'Returning'` (>1 session) |
| `has_converted` | SMALLINT | 1 if any session had revenue > 0 |
| `is_high_value` | SMALLINT | 1 if total_revenue_usd > 0 |
| `total_revenue_usd` | DECIMAL(12,2) | Sum of purchase revenue |
| `total_transactions` | INTEGER | Distinct transactions with revenue |
| `avg_revenue_per_session` | DECIMAL(10,2) | Revenue / sessions |
| `total_pageviews` | INTEGER | Count of page_view events |
| `unique_pages_visited` | INTEGER | Distinct URLs visited |
| `most_visited_page_title` | VARCHAR(512) | Page title with most page_views |
| `avg_pages_per_session` | DECIMAL(10,2) | Pageviews / sessions |
| `bounce_sessions` | INTEGER | Sessions with 1 page_view and no revenue |
| `bounce_rate` | DECIMAL(8,4) | bounce_sessions / total_sessions |
| `total_engagement_sec` | DECIMAL(14,2) | Sum of engagement_time_msec / 1000 |
| `avg_engagement_sec_per_session` | DECIMAL(10,2) | Engagement / sessions |
| `avg_session_number` | DECIMAL(8,2) | Average ga_session_number |
| `preferred_device` | VARCHAR(32) | Most frequent device_category |
| `preferred_platform` | VARCHAR(32) | Most frequent platform |
| `acquisition_source` | VARCHAR(256) | traffic_source from first session |
| `acquisition_medium` | VARCHAR(256) | traffic_medium from first session |
| `acquisition_campaign` | VARCHAR(256) | traffic_campaign from first session |
| `country` | VARCHAR(128) | Most frequent geo_country |
| `city` | VARCHAR(128) | Most frequent geo_city |
| `browser` | VARCHAR(128) | Most frequent device_browser |
| `operating_system` | VARCHAR(128) | Most frequent device_os |

---

## Project Structure

```
.
├── infra/
│   ├── cloudformation.yml          # Core stack: Redshift Serverless + IAM role for Spectrum
│   ├── trigger-pipeline.yml        # Pipeline stack: EventBridge + Step Functions + Lambda + SNS
│   ├── glue_etl_ga4.py             # PySpark ETL: Bronze → Silver
│   ├── lambda_gold_refresh.py      # Lambda: TRUNCATE + INSERT on Gold tables
│   ├── gold_layer.sql              # DDL for all Gold tables
│   ├── setup_ga4db.sql             # One-time DB setup (external schema, view, Gold tables)
│   ├── cleanup_dev.sql             # Drops all Gold objects from dev database
│   ├── iam_readonly_policy.json    # IAM policy for read-only data team access
│   ├── setup_team_iam.sh           # Creates IAM users + access keys for data team
│   └── tests/
│       ├── __init__.py
│       └── test_dim_visitor_profile.py  # 6 unit tests + 6 property-based tests
│
├── docs/
│   └── arquitectura-datos-ga4.md   # Full architecture reference
│
├── .github/
│   └── workflows/
│       ├── ci.yml                  # Run tests on every push/PR
│       ├── deploy-core.yml         # Deploy cloudformation.yml to AWS
│       └── deploy-pipeline.yml     # Deploy trigger-pipeline.yml to AWS
│
├── requirements.txt                # Python test dependencies
└── README.md
```

---

## Getting Started

### Prerequisites

- AWS CLI configured with a profile that has access to account `092908647087`
- Python 3.12+
- `pip install -r requirements.txt`

### First-time Setup

**1. Deploy core infrastructure** (Redshift Serverless + IAM role):

```bash
aws cloudformation deploy \
  --template-file infra/cloudformation.yml \
  --stack-name ga4-analytics \
  --parameter-overrides AdminPassword=<your-password> \
  --capabilities CAPABILITY_NAMED_IAM \
  --profile <your-profile>
```

**2. Deploy the automated pipeline** (EventBridge + Step Functions + Lambda + SNS):

```bash
aws cloudformation deploy \
  --template-file infra/trigger-pipeline.yml \
  --stack-name ga4-pipeline-trigger \
  --capabilities CAPABILITY_NAMED_IAM \
  --profile <your-profile>
```

**3. Initialize the database** — run `infra/setup_ga4db.sql` in Redshift Query Editor v2:
- Connect to workgroup `ga4-workgroup`, database `ga4db`
- Execute the full script (creates external schema, `ga4_analytics` view, and all Gold tables)

---

## CI/CD

Three GitHub Actions workflows handle the full lifecycle:

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | Push / PR to `main` or `develop` | Runs the full pytest suite (unit + property-based tests) |
| `deploy-core.yml` | Push to `main` touching `infra/cloudformation.yml` | Deploys the core CloudFormation stack to AWS |
| `deploy-pipeline.yml` | Push to `main` touching `infra/trigger-pipeline.yml` or `infra/lambda_gold_refresh.py` | Packages Lambda + deploys pipeline stack |

### Required GitHub Secrets

Set these in **Settings → Secrets and variables → Actions**:

| Secret | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM user access key with CloudFormation + Lambda deploy permissions |
| `AWS_SECRET_ACCESS_KEY` | Corresponding secret key |
| `REDSHIFT_ADMIN_PASSWORD` | Admin password for Redshift (used in core stack deploy) |

---

## Running Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Run full test suite
pytest infra/tests/test_dim_visitor_profile.py -v

# Run only unit tests
pytest infra/tests/test_dim_visitor_profile.py -v -k "not prop"

# Run only property-based tests
pytest infra/tests/test_dim_visitor_profile.py -v -k "prop"
```

Expected output: **12 passed** (6 unit tests + 6 property-based tests with 100 examples each).

---

## Deployment

### Manual Lambda deploy

```bash
# Package
cd infra && zip lambda_gold_refresh.zip lambda_gold_refresh.py

# Deploy
aws lambda update-function-code \
  --function-name ga4-gold-refresh \
  --zip-file fileb://infra/lambda_gold_refresh.zip \
  --profile <your-profile>
```

### Manual pipeline trigger

```bash
aws lambda invoke \
  --function-name ga4-gold-refresh \
  --payload '{}' response.json \
  --profile <your-profile>
```

### Run Glue ETL manually

```bash
aws glue start-job-run --job-name ga4-flatten-etl --profile <your-profile>
```

---

## Connecting BI Tools

### Power BI

| Parameter | Value |
|---|---|
| Connection type | Amazon Redshift |
| Server | `ga4-workgroup.092908647087.us-east-1.redshift-serverless.amazonaws.com` |
| Port | `5439` |
| Database | `ga4db` |
| Schema | `gold` |
| Recommended tables | `dim_visitor_profile`, `dim_users`, `dim_sessions`, `fact_daily_events`, `fact_page_performance` |
| Driver | Amazon Redshift ODBC Driver v2 |

### QuickSight

Use the Redshift connector pointing to the same workgroup/database/schema. All Gold tables are accessible via the `PUBLIC` grant.

---

## AWS Resources

| Service | Name | Purpose |
|---|---|---|
| S3 | `google-analytics-80oeste` | Bronze — raw GA4 Parquet exports |
| S3 | `google-analytics-80oeste-curated` | Silver — flattened, partitioned Parquet |
| Glue ETL Job | `ga4-flatten-etl` | Bronze → Silver transformation |
| Glue Crawler | `ga4-curated-crawler` | Catalogs Silver data (runs 6am daily) |
| Redshift Serverless | `ga4-workgroup` / `ga4-analytics` | Query engine, 8 RPU, database `ga4db` |
| Redshift Spectrum | `ga4_spectrum` | Queries Silver S3 data from Redshift |
| Lambda | `ga4-gold-refresh` | Refreshes Gold layer (Python 3.12) |
| Step Functions | `ga4-etl-pipeline` | Orchestrates: Glue ETL → Crawler → Lambda |
| EventBridge | `ga4-s3-parquet-trigger` | Triggers pipeline on new `.parquet` in raw bucket |
| SNS | `ga4-pipeline-notifications` | Email alerts at each pipeline step |
| CloudFormation | `ga4-analytics` + `ga4-pipeline-trigger` | IaC for all resources |
| IAM | `redshift-spectrum-ga4` | Redshift → S3 + Glue access |

---

## Account Info

- **AWS Account (80oeste / GA4):** `092908647087`
- **Region:** `us-east-1`
- **Notification email:** `lcalvoa@80oeste.com`

---

*Built with [Kiro](https://kiro.dev) — spec-driven development with property-based testing.*
