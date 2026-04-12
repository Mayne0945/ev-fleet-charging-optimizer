# EV Fleet Charging Optimizer & Grid Load Prediction System

Production-grade serverless cloud data engineering system — real-time EV fleet telemetry, Medallion data lake, intelligent charging optimisation, containerised ML demand forecasting, and live operational dashboard. Fully deployed on AWS, managed via Terraform IaC, with CI/CD via GitHub Actions.

## Stack

| Category | Technology |
|---|---|
| Cloud | AWS eu-west-1 (Ireland) |
| IaC | Terraform hashicorp/aws v6.35.1 |
| CI/CD | GitHub Actions — OIDC, plan on PR, apply on merge |
| Ingestion | Amazon SQS Standard + DLQ |
| Compute | AWS Lambda Python 3.11 |
| Forecaster | Docker on Amazon ECR — Prophet 1.1.5, CmdStan 2.38.0 |
| Storage | S3 Bronze / Silver / Gold (Medallion Architecture) |
| Schema Registry | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| State Store | Amazon DynamoDB — PAY_PER_REQUEST, 24hr TTL |
| Scheduling | Amazon EventBridge |
| ML Framework | Meta Prophet 1.1.5 + CmdStan 2.38.0 C++ backend |
| Dashboard | Grafana OSS — Athena plugin |
| Simulation | Python asyncio + boto3 |

## Architecture

Fleet Simulation (10 EVBus coroutines, 600x time multiplier)
→ SQS Queue + DLQ
→ Lambda Ingestor → S3 Bronze (raw JSON archive)
→ Lambda Transformer → S3 Silver (Parquet, partitioned year/month/day)
→ AWS Glue + Athena (SQL query engine)
→ Lambda Gold Aggregator → S3 Gold (fleet snapshots, every 5 min)
→ Lambda Optimizer (DLM allocation, per-vehicle decisions, every 5 min)
→ Lambda Forecaster on ECR (Prophet 24hr forecast, every 1 hr)
→ Grafana OSS Dashboard (8 panels, Athena plugin)

## Optimizer Decision Tree

| Priority | Decision | Condition |
|---|---|---|
| 1 | DO_NOT_CHARGE | battery_temp >= 45C |
| 2 | EMERGENCY_RETURN | en_route AND SOC <= 10% |
| 3 | STANDBY (transit) | moving and not connected |
| 4 | CHARGE_NOW | urgency >= 60 AND grid has headroom |
| 5 | QUEUE_FOR_CHARGING | urgency >= 60 AND grid at capacity |
| 6 | CONTINUE_CHARGING | connected AND medium+ urgency |
| 7 | DEFER_CHARGING | peak tariff AND urgency < 40 |
| 8 | STANDBY | default |

Urgency formula: (time_pressure x 0.6) + (soc_weight x 0.4), range 0-100
Grid capacity: 150kW depot limit enforced on every optimizer run

## Key Engineering Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Ingestion buffer | SQS not Kinesis | One consumer, pay-per-message, no idle shard cost |
| Silver format | Parquet + Snappy | Columnar, compressed, Athena partition pruning |
| Gold reads DynamoDB not Athena | Single source of truth | Dashboard always agrees with optimizer |
| Lambda container for Prophet | Docker on ECR | CmdStan C++ exceeds 250MB zip limit |
| Grafana over QuickSight | OSS + Athena plugin | QuickSight per-session pricing prohibitive |
| Terraform from day one | Not a future enhancement | All resources version-controlled |

## Bugs Solved

| Bug | Root Cause | Fix |
|---|---|---|
| DLM invisible to 2 buses | Boolean True vs string true in DynamoDB filter | .eq(True) throughout |
| Simulator ignored optimizer | No DynamoDB read in fleet manager loop | Added DLM sync coroutine |
| Gold and Optimizer disagreed | Duplicate decision engine in Gold Aggregator | Removed, Gold reads from DynamoDB |
| Timeline fracture at 600x speed | EventBridge 5-min = 3000 simulated minutes | Fleet manager invokes optimizer every 30s |
| Prophet data inflated 500x | Naive SUM across hundreds of snapshots per hour | Two-level aggregation: AVG per vehicle, SUM across |
| Prophet stale forecast | charger_kw > 0 excluded idle periods | Removed filter + resample timeline anchor |

## Author

Tshifhiwa Gift Moila — Cloud Data Engineer
Johannesburg, South Africa — April 2026
