# EV Fleet Charging Optimizer & Grid Load Prediction System

![AWS](https://img.shields.io/badge/AWS-Serverless-FF9900?style=flat&logo=amazonaws&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-IaC-7B42BC?style=flat&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![Prophet](https://img.shields.io/badge/ML-Prophet-00A3E0?style=flat)
![Grafana](https://img.shields.io/badge/Dashboard-Grafana-F46800?style=flat&logo=grafana&logoColor=white)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions_OIDC-2088FF?style=flat&logo=githubactions&logoColor=white)

---

## The Problem

When a large EV fleet returns to depot simultaneously, every vehicle plugging in at full power creates a demand spike that risks tripping the grid, incurring peak tariff penalties, and leaving the lowest-priority buses undercharged for the next shift.

This system solves that. It ingests live vehicle telemetry, predicts grid demand 24 hours ahead, and automatically allocates charging power across the fleet — prioritising by urgency, deferring during peak tariff windows, and never exceeding depot capacity.

---

## Dashboard

![Main Operations](docs/dashboard_main.png)
*Real-time Grid Load, Fleet SOC, and Prophet Demand Forecast*

![System Details](docs/dashboard_details.png)
*Battery Thermal Monitoring and Tariff Period tracking*

---

## Quick Start

**Infrastructure**
```bash
git clone https://github.com/Mayne0945/ev-fleet-charging-optimizer.git
cd ev-fleet-charging-optimizer
terraform init
terraform apply
```

**Run the simulation**
```bash
export AWS_PROFILE=personal
aws events enable-rule --name ev-fleet-gold-schedule --region eu-west-1
aws events enable-rule --name ev-fleet-optimizer-schedule --region eu-west-1
python3 simulation/fleet_manager.py
```

> Estimated AWS cost: under $5/month at simulation scale. EventBridge schedules are disabled between sessions to eliminate idle cost.

---

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
| Data Format | JSON (Bronze) → Parquet Snappy (Silver) → JSON snapshots (Gold) |
| Schema Registry | AWS Glue Data Catalog |
| Query Engine | Amazon Athena |
| State Store | Amazon DynamoDB — PAY_PER_REQUEST, 24hr TTL |
| Scheduling | Amazon EventBridge |
| ML Framework | Meta Prophet 1.1.5 + CmdStan 2.38.0 C++ backend |
| Dashboard | Grafana OSS — Athena plugin |
| Simulation | Python asyncio + boto3 |

---

## Architecture

```
Fleet Simulation (10 EVBus coroutines, 600x time multiplier)
  → SQS Queue + DLQ (guaranteed delivery)
  → Lambda Ingestor → S3 Bronze (raw JSON archive)
  → Lambda Transformer → S3 Silver (Parquet, partitioned year/month/day)
  → AWS Glue + Athena (SQL query engine)
  → Lambda Gold Aggregator → S3 Gold (fleet snapshots, every 5 min)
  → Lambda Optimizer (DLM allocation, per-vehicle decisions, every 5 min)
  → Lambda Forecaster on ECR (Prophet 24hr forecast, every 1 hr)
  → Grafana OSS Dashboard (8 panels, Athena plugin)
```

---

## Optimizer and Dynamic Load Management

On every optimizer run, a DLM allocation pass executes before the decision tree. It reads all vehicles from DynamoDB, ranks them by urgency score descending, and distributes the 150kW depot budget proportionally by urgency weight. Each CHARGE_NOW assignment reduces the remaining budget before the next vehicle is evaluated — preventing simultaneous over-allocation. Vehicles that cannot be served receive QUEUE_FOR_CHARGING and are re-evaluated on the next run.

**Decision Tree (Priority Order)**

| Priority | Decision | Condition |
|---|---|---|
| 1 | DO_NOT_CHARGE | battery_temp >= 45°C — thermal safety override |
| 2 | EMERGENCY_RETURN | en_route AND SOC <= 10% |
| 3 | STANDBY (transit) | moving and not connected — in-transit short-circuit |
| 4 | CHARGE_NOW | urgency >= 60 AND grid has headroom |
| 5 | QUEUE_FOR_CHARGING | urgency >= 60 AND grid at capacity |
| 6 | CONTINUE_CHARGING | connected AND medium+ urgency |
| 7 | DEFER_CHARGING | peak tariff AND urgency < 40 |
| 8 | STANDBY | default — SOC healthy, no action required |

**Urgency formula:** `(time_pressure × 0.6) + (soc_weight × 0.4)` — range 0–100

**Peak tariff evasion:** During peak hours (07:00–10:00 and 17:00–21:00 SAST), effective depot capacity drops to 30kW. Only vehicles with urgency >= 80 break through. Low-urgency vehicles wait for off-peak windows automatically.

**Li-ion charging curve:** The fleet manager simulates real battery physics — full CC-phase rate from 0–80% SOC, linear taper to 20% of rate from 80–95%, trickle-only above 95%. The battery will never be hammered at full power near capacity.

---

## Demand Forecasting

Containerised Lambda (Docker on ECR) queries 30 days of Silver telemetry via two-level Athena aggregation, fits a Prophet model with CmdStan C++ backend, and writes a 24-hour-ahead demand forecast with 80% confidence intervals to Gold S3 every hour.

**Why Docker:** CmdStan C++ backend exceeds the 250MB Lambda zip limit. Container images support up to 10GB.

**Training data:** Naive SUM of `charger_kw` inflates values 50–500x across hundreds of telemetry snapshots per hour. Correct approach: AVG per vehicle per hour (inner query), SUM across vehicles (outer query). Max realistic depot output: 10 vehicles × 50kW = 500kW.

**South African context:**
- `add_country_holidays(country_name='ZA')` — Prophet treats public holidays as normal workdays without this, wildly overestimating load on days buses don't run
- Custom daily seasonality (`fourier_order=10`) — captures sharp morning/evening EV bus shift patterns that the default `fourier_order=4` misses
- Training data converted UTC → SAST before model fit — ensures learned patterns align with local tariff windows, not UTC offsets

---

## Grafana Dashboard

| Panel | Type | Source |
|---|---|---|
| Fleet State of Charge (%) | Bar gauge | Silver |
| Depot Grid Load (kW) | Gauge | Silver |
| Fleet Vehicle Status | Table | Silver |
| Current Tariff Period | Stat tile | Silver |
| Active Charging kW per Vehicle | Bar chart | Silver |
| Fleet SOC Over Time | Time series | Silver |
| Battery Temperature | Gauge | Silver |
| 24-Hour Demand Forecast | Time series | Gold |

Vehicle dropdown variable enables per-bus drill-down across all relevant panels.

---

## CI/CD Pipeline

```
Pull Request  →  terraform plan  →  plan output posted as PR comment
Merge to main →  terraform apply →  infrastructure updated automatically
```

Authentication via OIDC. No long-lived AWS access keys stored in GitHub secrets.

---

## Key Engineering Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Ingestion buffer | SQS not Kinesis | One consumer, pay-per-message, no idle shard cost |
| Silver format | Parquet + Snappy | Columnar storage, Athena predicate pushdown |
| DynamoDB billing | PAY_PER_REQUEST | No idle cost, auto-scales with fleet size |
| Gold reads DynamoDB not Athena | Single source of truth | Dashboard always agrees with optimizer |
| Lambda container for Prophet | Docker on ECR | CmdStan C++ exceeds 250MB zip limit |
| Grafana over QuickSight | OSS + Athena plugin | QuickSight per-session pricing prohibitive at development scale |
| Terraform from day one | Not a future enhancement | All resources version-controlled from first commit |
| DLM inside optimizer.py | Not a separate Lambda | Optimizer holds all state — separate Lambda adds cold start latency with no benefit |
| Safe-start fallback | 7kW floor after 5min | Prevents buses sitting empty at departure if optimizer is slow to respond |

---

## Bugs Solved

| Bug | Root Cause | Fix |
|---|---|---|
| DLM always 0kW | Boolean `True` != string `"true"` in DynamoDB filter | `.eq(True)` throughout |
| Simulator ignored optimizer | No DynamoDB read in fleet manager loop | Added DLM sync coroutine every 5 ticks |
| Gold and Optimizer disagreed | Duplicate decision engine in Gold Aggregator | Removed — Gold reads from DynamoDB |
| Timeline fracture at 600x | EventBridge 5-min = 3000 simulated minutes | Fleet manager invokes optimizer every 30 real seconds |
| Prophet data inflated 500x | Naive SUM across hundreds of snapshots per hour | Two-level aggregation: AVG per vehicle, SUM across vehicles |
| Prophet stale forecast | `charger_kw > 0` excluded idle periods | Removed filter, resampled timeline anchor to current hour |
| Prophet timezone error | UTC metadata in datetime column | `.dt.tz_localize(None)` before `model.fit()` |
| Lambda handler mismatch post CI/CD | File renamed and handler updated in separate commits | Always rename file and update handler in the same commit |
| SQS silent failures | Response not captured — fire and forget | Capture `MessageId` — no ID means the message does not exist |
| SOC dropping 25% per tick | Tick interval too large at 600x multiplier | Reduced interval from 1–3s to 0.1–0.5s for smooth physics |

---

## Author

**Tshifhiwa Gift Moila** — Cloud Data Engineer
Johannesburg, South Africa — April 2026
[GitHub](https://github.com/Mayne0945) · [LinkedIn](https://linkedin.com/in/your-profile)