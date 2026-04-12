# EV Fleet Charging Optimizer & Grid Load Prediction System

> **Production-grade serverless cloud data engineering system** — real-time EV fleet telemetry, Medallion data lake, intelligent charging optimisation, containerised ML demand forecasting, and live operational dashboard. Fully deployed on AWS, managed via Terraform IaC, with CI/CD via GitHub Actions.

---

## Architecture Overview
Fleet Simulation (Python asyncio · 10 EVBus coroutines · 600x time multiplier)
│
▼
SQS Standard Queue + DLQ          ← guaranteed delivery, no dropped payloads
│
▼
Lambda: Ingestor
│  validates payload, writes raw JSON
▼
S3 Bronze Layer                    ← immutable raw telemetry archive
│
│  S3 ObjectCreated trigger (automatic)
▼
Lambda: Transformer
│  21-field schema validation, Snappy Parquet conversion
▼
S3 Silver Layer                    ← partitioned Parquet (year/month/day)
AWS Glue Data Catalog + Athena     ← SQL query engine over Silver
│
│  EventBridge rate(5 min)
▼
Lambda: Gold Aggregator            ← reads DynamoDB → writes fleet_snapshot
│
▼
S3 Gold Layer                      ← pre-computed fleet snapshots
│
├──── EventBridge rate(5 min) ────▶ Lambda: Optimizer
│                                         │  DLM allocation
│                                         │  per-vehicle decisions
│                                         ▼
│                                   Amazon DynamoDB
│                                   (real-time fleet state)
│
└──── EventBridge rate(1 hr) ─────▶ Lambda: Forecaster (Docker/ECR)
│  Prophet + CmdStan C++
│  30-day rolling window
▼
Gold S3: forecast/latest.json
│
▼
Grafana OSS (Athena plugin)
8-panel live operations dashboard

---

## Technology Stack

| Category | Technology |
|---|---|
| Cloud | AWS eu-west-1 (Ireland) |
| Infrastructure as Code | Terraform hashicorp/aws v6.35.1 — modular IaC |
| CI/CD | GitHub Actions — OIDC, plan on PR, apply on merge |
| Ingestion | Amazon SQS Standard + DLQ |
| Compute | AWS Lambda Python 3.11 |
| Forecaster | Docker image on Amazon ECR — Prophet 1.1.5, CmdStan 2.38.0 |
| Storage | S3 Bronze / Silver / Gold (Medallion Architecture) |
| Data Format | JSON (Bronze) → Parquet Snappy (Silver) → JSON snapshots (Gold) |
| Schema Registry | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| State Store | Amazon DynamoDB — PAY_PER_REQUEST, 24hr TTL |
| Scheduling | Amazon EventBridge |
| ML Framework | Meta Prophet 1.1.5 + CmdStan 2.38.0 C++ backend |
| Dashboard | Grafana OSS — Athena plugin |
| Simulation | Python asyncio + boto3 |

---

## Core Capabilities

### Tariff-Aware Charging Optimisation
Charging schedules shift energy consumption toward off-peak tariff windows while guaranteeing vehicle readiness before scheduled departures. During peak hours (07:00–10:00 and 17:00–21:00 SAST), effective depot capacity is reduced to 30kW — only vehicles with urgency score ≥ 80 break through.

### Dynamic Load Management (DLM)
Grid capacity is distributed proportionally across charging vehicles by urgency score. Thermal throttling (0kW at ≥ 45°C, 50% at ≥ 38°C) and headroom redistribution ensure the 150kW depot limit is never breached.

**Urgency formula:** (time_pressure x 0.6) + (soc_weight x 0.4) — range 0-100

### Optimizer Decision Tree (Priority Order)
| Priority | Decision | Condition |
|---|---|---|
| 1 | DO_NOT_CHARGE | battery_temp >= 45°C |
| 2 | EMERGENCY_RETURN | en_route AND SOC <= 10% |
| 3 | STANDBY (transit) | moving and not connected |
| 4 | CHARGE_NOW | urgency >= 60 AND grid has headroom |
| 5 | QUEUE_FOR_CHARGING | urgency >= 60 AND grid at capacity |
| 6 | CONTINUE_CHARGING | connected AND medium+ urgency |
| 7 | DEFER_CHARGING | peak tariff AND urgency < 40 |
| 8 | STANDBY | default — SOC healthy |

### Prophet Demand Forecasting
A containerised Lambda (Docker on ECR) queries 30 days of Silver telemetry via a two-level Athena aggregation, fits a Prophet model with CmdStan C++ backend, and produces a 24-hour-ahead electricity demand forecast with 80% confidence intervals. Grid breach detection confirmed at 252.9kW vs 150kW physical limit.

**Why Docker?** Prophet's CmdStan C++ backend exceeds the 250MB Lambda zip limit. Container images allow up to 10GB — the only viable deployment path.

---

## Medallion Data Lake

### Bronze — Raw Telemetry Archive
Immutable, append-only raw JSON. Every payload stored exactly as received. S3 ObjectCreated event auto-triggers Silver transformation.

### Silver — Validated Telemetry
21-field schema enforced by Lambda Transformer. Snappy-compressed Parquet, partitioned by year/month/day. Queryable via Athena at any granularity.

### Gold — Business-Ready Snapshots
Pre-computed datasets refreshed every 5 minutes. Gold Aggregator reads DynamoDB — not Silver — ensuring the dashboard always agrees with optimizer decisions.

---

## Terraform Module Structure
ev-fleet-charging-optimizer/
├── main.tf
├── variables.tf
├── outputs.tf
└── modules/
├── lakehouse/
├── database/
└── compute/
├── gold_layer.tf
├── optimizer.tf
└── forecaster.tf

---

## CI/CD Pipeline
Pull Request → terraform plan → plan output posted as PR comment
Merge to main → terraform apply → infrastructure updated automatically

Authentication via OIDC — no long-lived AWS access keys stored in GitHub secrets.

---

## Grafana Dashboard

8 panels connected to Amazon Athena via the Athena data source plugin. Vehicle dropdown variable for per-bus drill-down.

---

## Key Engineering Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Ingestion buffer | SQS Standard (not Kinesis) | One consumer, pay-per-message, no idle shard cost |
| Silver format | Parquet + Snappy | Columnar, compressed, Athena partition pruning |
| DynamoDB billing | PAY_PER_REQUEST | No idle cost, auto-scales |
| Gold reads DynamoDB not Athena | Single source of truth | Dashboard always agrees with optimizer |
| Lambda container for Prophet | Docker on ECR | CmdStan C++ exceeds 250MB zip limit |
| Grafana over QuickSight | OSS + Athena plugin | QuickSight per-session pricing prohibitive |
| Terraform from day one | Not a future enhancement | All resources version-controlled, reproducible |

---

## Bugs Solved

| Bug | Root Cause | Fix |
|---|---|---|
| DLM invisible to 2 of 10 buses | Boolean True vs string 'true' in DynamoDB filter | .eq(True) and bool() parsing throughout |
| Simulator ignored optimizer | No DynamoDB read in fleet manager update loop | Added DLM sync coroutine every 5 ticks |
| Gold and Optimizer disagreed | Duplicate decision engine in Gold Aggregator | Removed — Gold reads optimizer fields from DynamoDB |
| Timeline fracture at 600x speed | EventBridge 5-min = 3,000 simulated minutes | Fleet manager explicitly invokes optimizer every 30s |
| Prophet training data inflated 500x | Naive SUM across hundreds of snapshots per hour | Two-level aggregation: AVG per vehicle, SUM across vehicles |
| Prophet stale forecast | charger_kw > 0 filter excluded idle periods | Removed filter + timeline anchor via resample to current hour |

---

## Author

**Tshifhiwa Gift Moila** — Cloud Data Engineer
Johannesburg, South Africa
April 2026
