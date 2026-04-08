import boto3
import json
import os
import datetime

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
s3       = boto3.client('s3', region_name='eu-west-1')

GOLD_BUCKET      = os.environ['GOLD_BUCKET_NAME']
FLEET_TABLE      = os.environ['DYNAMODB_TABLE_NAME']

# Constants for metrics computation
BATTERY_CAPACITY_KWH = 120.0
GRID_CAPACITY_KW     = 150.0
CHARGER_KW           = {"slow": 7.0, "fast": 22.0, "rapid": 50.0}


# ─────────────────────────────────────────────────────────────
# METRICS — fleet-level analytics only
# No recommendation logic here — that belongs to the optimizer
# ─────────────────────────────────────────────────────────────
def compute_energy_gap(soc: float, allocated_kw: float) -> dict:
    """
    How much energy is needed to reach target SOC,
    and how long at the current DLM-allocated rate.
    Uses allocated_kw from optimizer — not a fixed charger assumption.
    """
    target_soc      = 95.0
    soc_gap         = max(0.0, target_soc - soc)
    kwh_needed      = (soc_gap / 100.0) * BATTERY_CAPACITY_KWH
    effective_kw    = allocated_kw if allocated_kw > 0 else 22.0
    hours_to_full   = kwh_needed / effective_kw
    return {
        "kwh_needed":      round(kwh_needed, 2),
        "hours_to_full":   round(hours_to_full, 2),
        "minutes_to_full": round(hours_to_full * 60, 1),
    }


def compute_thermal_health(battery_temp_c: float) -> dict:
    """
    Battery thermal status — informational metric for dashboard.
    Throttling decisions are made by the optimizer, not here.
    """
    if battery_temp_c >= 45.0:
        return {"thermal_status": "critical", "safe_to_charge": False,  "max_charge_kw": 0.0}
    elif battery_temp_c >= 38.0:
        return {"thermal_status": "warning",  "safe_to_charge": True,   "max_charge_kw": 7.0}
    else:
        return {"thermal_status": "healthy",  "safe_to_charge": True,   "max_charge_kw": 50.0}


def compute_tariff_risk(tariff_period: str, urgency_score: float) -> dict:
    """
    Tariff risk metric — informational only.
    The optimizer has already made the defer/charge decision.
    This surfaces the cost context for the Grafana dashboard.
    """
    cost_level   = {"peak": 3, "standard": 2, "off_peak": 1}.get(tariff_period, 2)
    should_defer = cost_level == 3 and urgency_score < 40
    return {
        "tariff_cost_level": cost_level,
        "should_defer":      should_defer,
        "defer_reason":      "Peak tariff — low urgency" if should_defer else None,
    }


# ─────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────
def handler(event, context):
    now   = datetime.datetime.now(datetime.timezone.utc)
    today = now.strftime("%Y-%m-%d")

    print(f"🏅 Gold aggregation started | {now.isoformat()}")

    # ── Step 1: Read all vehicles from DynamoDB ───────────────
    # DynamoDB is the single source of truth for current fleet state.
    # The transformer writes vehicle telemetry here.
    # The optimizer then overwrites with decisions + allocated_kw.
    # Both are merged — no need for a separate Athena query.
    table    = dynamodb.Table(FLEET_TABLE)
    response = table.scan()
    vehicles = response.get("Items", [])

    if not vehicles:
        print("⚠️  No vehicles in DynamoDB — skipping Gold write")
        return {"statusCode": 200, "body": "No data"}

    print(f"✅ Retrieved {len(vehicles)} vehicles from DynamoDB")

    # ── Step 2: Build fleet snapshot ─────────────────────────
    fleet_snapshot  = []
    total_grid_load = 0.0

    for v in vehicles:
        try:
            vid               = v.get("vehicle_id")
            soc               = float(v.get("state_of_charge", 0))
            battery_temp      = float(v.get("battery_temp_c", 25))
            charger_kw        = float(v.get("charger_kw", 0))
            allocated_kw      = float(v.get("allocated_kw", 0))
            time_to_departure = float(v.get("time_to_departure_min", 999))
            tariff_period     = v.get("tariff_period", "standard")
            charger_connected = bool(v.get("charger_connected", False))
            status            = v.get("status", "unknown")
            target_soc        = float(v.get("target_soc", 95.0))

            # ── Optimizer decisions — read directly, never recalculate ──
            # These were written by optimizer.py — trust them
            recommendation  = v.get("optimizer_recommendation", "UNKNOWN")
            reason          = v.get("optimizer_reason", "No optimizer run yet")
            urgency_score   = float(v.get("optimizer_urgency_score", 0))
            urgency_level   = v.get("optimizer_urgency_level", "unknown")
            optimizer_run_at = v.get("optimizer_run_at", "never")

            # ── Gold metrics — analytics only, not decisions ──────────
            energy_gap    = compute_energy_gap(soc, allocated_kw)
            thermal       = compute_thermal_health(battery_temp)
            tariff_risk   = compute_tariff_risk(tariff_period, urgency_score)

            # Grid load — sum from DLM allocated_kw, not raw charger_kw
            if charger_connected and allocated_kw > 0:
                total_grid_load += allocated_kw

            fleet_snapshot.append({
                "vehicle_id":            vid,
                "snapshot_time":         now.isoformat() + "Z",
                "date":                  today,
                # ── Current state from transformer ────────────────────
                "state_of_charge":       soc,
                "target_soc":            target_soc,
                "status":                status,
                "charger_connected":     charger_connected,
                "charger_kw":            charger_kw,
                "allocated_kw":          allocated_kw,
                "battery_temp_c":        battery_temp,
                "tariff_period":         tariff_period,
                "time_to_departure_min": time_to_departure,
                "next_departure":        v.get("next_departure", ""),
                "lat":                   v.get("lat", ""),
                "long":                  v.get("long", ""),
                # ── Optimizer decisions — sourced from DynamoDB ───────
                "recommendation":        recommendation,
                "reason":                reason,
                "urgency_score":         urgency_score,
                "urgency_level":         urgency_level,
                "optimizer_run_at":      optimizer_run_at,
                # ── Gold analytics metrics ────────────────────────────
                "energy_gap":            energy_gap,
                "thermal_health":        thermal,
                "tariff_risk":           tariff_risk,
            })

        except Exception as e:
            print(f"❌ Failed to process {v.get('vehicle_id')}: {str(e)}")
            continue

    # ── Step 3: Fleet-level summary ───────────────────────────
    grid_utilization = round((total_grid_load / GRID_CAPACITY_KW) * 100, 1)

    gold_output = {
        "snapshot_time": now.isoformat() + "Z",
        "date":          today,
        "fleet_summary": {
            "total_vehicles":       len(fleet_snapshot),
            "vehicles_charging":    sum(1 for v in fleet_snapshot
                                        if v["charger_connected"]),
            "vehicles_en_route":    sum(1 for v in fleet_snapshot
                                        if v["status"] == "en_route"),
            "vehicles_idle":        sum(1 for v in fleet_snapshot
                                        if v["status"] == "at_depot_idle"),
            "avg_soc":              round(
                                        sum(v["state_of_charge"] for v in fleet_snapshot)
                                        / len(fleet_snapshot), 1
                                    ),
            "total_grid_load_kw":   round(total_grid_load, 1),
            "grid_utilization_pct": grid_utilization,
            "grid_status":          (
                                        "critical" if grid_utilization >= 90 else
                                        "warning"  if grid_utilization >= 70 else
                                        "healthy"
                                    ),
            "tariff_period":        fleet_snapshot[0]["tariff_period"]
                                    if fleet_snapshot else "unknown",
            "critical_vehicles":    [
                                        v["vehicle_id"] for v in fleet_snapshot
                                        if v["urgency_level"] == "critical"
                                    ],
            "defer_vehicles":       [
                                        v["vehicle_id"] for v in fleet_snapshot
                                        if v["recommendation"] == "DEFER_CHARGING"
                                    ],
        },
        "vehicles": fleet_snapshot,
    }

    # ── Step 4: Write to Gold S3 ──────────────────────────────
    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key="fleet_snapshot/latest.json",
        Body=json.dumps(gold_output, indent=2, default=str),
        ContentType="application/json"
    )

    # Archive timestamped copy for historical analysis
    archive_key = f"fleet_snapshot/archive/{today}/{now.strftime('%H-%M-%S')}.json"
    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key=archive_key,
        Body=json.dumps(gold_output, indent=2, default=str),
        ContentType="application/json"
    )

    print(f"✅ Gold snapshot written | {len(fleet_snapshot)} vehicles | "
          f"Grid: {grid_utilization}% | "
          f"Critical: {gold_output['fleet_summary']['critical_vehicles']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "vehicles_processed": len(fleet_snapshot),
            "grid_utilization":   grid_utilization,
            "snapshot_key":       "fleet_snapshot/latest.json",
        })
    }