import boto3
import json
import os
import datetime
import time

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
athena  = boto3.client('athena', region_name='eu-west-1')
s3      = boto3.client('s3', region_name='eu-west-1')

GOLD_BUCKET      = os.environ['GOLD_BUCKET_NAME']
ATHENA_WORKGROUP = os.environ['ATHENA_WORKGROUP']
DATABASE         = os.environ['GLUE_DATABASE']

# Charging constants
BATTERY_CAPACITY_KWH = 120.0
CHARGER_KW = {"slow": 7.0, "fast": 22.0, "rapid": 50.0}
GRID_CAPACITY_KW = 150.0  # Max depot load in kW

# ─────────────────────────────────────────────────────────────
# ATHENA QUERY RUNNER
# ─────────────────────────────────────────────────────────────
def run_athena_query(sql: str) -> list:
    """Execute SQL and return results as list of dicts."""
    now = datetime.datetime.now(datetime.timezone.utc)

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=ATHENA_WORKGROUP
    )
    execution_id = response["QueryExecutionId"]

    # Poll until complete
    for _ in range(30):
        time.sleep(2)
        status = athena.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(
                QueryExecutionId=execution_id
            )["QueryExecution"]["Status"]["StateChangeReason"]
            raise Exception(f"Athena query failed: {reason}")

    # Fetch results
    results   = athena.get_query_results(QueryExecutionId=execution_id)
    rows      = results["ResultSet"]["Rows"]
    headers   = [col["VarCharValue"] for col in rows[0]["Data"]]
    data      = []

    for row in rows[1:]:
        values = [col.get("VarCharValue", None) for col in row["Data"]]
        data.append(dict(zip(headers, values)))

    return data


# ─────────────────────────────────────────────────────────────
# GOLD METRICS COMPUTATION
# ─────────────────────────────────────────────────────────────
def compute_energy_gap(soc: float, charger_kw: float) -> dict:
    """
    How much energy (kWh) is needed to reach 95% SOC,
    and how long will it take at the current charger rate.
    """
    target_soc    = 95.0
    soc_gap       = max(0.0, target_soc - soc)
    kwh_needed    = (soc_gap / 100.0) * BATTERY_CAPACITY_KWH
    hours_to_full = kwh_needed / charger_kw if charger_kw > 0 else 999.0
    return {
        "kwh_needed":        round(kwh_needed, 2),
        "hours_to_full":     round(hours_to_full, 2),
        "minutes_to_full":   round(hours_to_full * 60, 1)
    }


def compute_urgency_score(soc: float, time_to_departure_min: float,
                           charger_kw: float) -> dict:
    """
    Urgency score 0-100. Higher = more urgent to charge now.
    Formula: weighted combination of SOC gap and time pressure.
    """
    target_soc        = 95.0
    soc_gap           = max(0.0, target_soc - soc)
    kwh_needed        = (soc_gap / 100.0) * BATTERY_CAPACITY_KWH
    minutes_to_full   = (kwh_needed / charger_kw * 60) if charger_kw > 0 else 999.0

    # Time pressure: how tight is the window?
    if time_to_departure_min <= 0:
        time_pressure = 100.0
    elif minutes_to_full >= time_to_departure_min:
        time_pressure = 100.0  # Can't fully charge in time
    else:
        time_pressure = (minutes_to_full / time_to_departure_min) * 100

    # SOC weight: lower SOC = higher urgency
    soc_weight = (soc_gap / target_soc) * 100

    # Combined urgency score
    urgency = round((time_pressure * 0.6) + (soc_weight * 0.4), 1)
    urgency = min(100.0, urgency)

    return {
        "urgency_score":   urgency,
        "urgency_level":   "critical" if urgency >= 80
                           else "high" if urgency >= 60
                           else "medium" if urgency >= 40
                           else "low",
        "can_fully_charge": minutes_to_full <= time_to_departure_min
    }


def compute_tariff_risk(tariff_period: str, urgency_score: float) -> dict:
    """
    Should we charge now or wait for cheaper electricity?
    Only defer if urgency is low AND we're in peak period.
    """
    tariff_cost = {"peak": 3, "standard": 2, "off_peak": 1}
    cost_level  = tariff_cost.get(tariff_period, 2)

    # Don't defer if urgent
    should_defer = (cost_level == 3 and urgency_score < 40)

    return {
        "tariff_cost_level": cost_level,
        "should_defer":      should_defer,
        "defer_reason":      "Peak tariff — low urgency, wait for off-peak"
                             if should_defer else None
    }


def compute_thermal_health(battery_temp_c: float) -> dict:
    """
    Is the battery safe to charge at full rate?
    """
    if battery_temp_c >= 45.0:
        status = "critical"
        safe_to_charge = False
        max_charge_kw  = 0.0
    elif battery_temp_c >= 38.0:
        status = "warning"
        safe_to_charge = True
        max_charge_kw  = 7.0   # Throttle to slow charge only
    else:
        status = "healthy"
        safe_to_charge = True
        max_charge_kw  = 50.0  # Full rate allowed

    return {
        "thermal_status":  status,
        "safe_to_charge":  safe_to_charge,
        "max_charge_kw":   max_charge_kw
    }


# ─────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────
def handler(event, context):
    now     = datetime.datetime.now(datetime.timezone.utc)
    today   = now.strftime("%Y-%m-%d")
    year    = now.strftime("%Y")
    month   = now.strftime("%m")
    day     = now.strftime("%d")

    print(f"🏅 Gold aggregation started | {now.isoformat()}")

    # ── Step 1: Get latest record per vehicle from Silver ─────
    sql = f"""
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY vehicle_id
                    ORDER BY timestamp DESC
                ) AS row_rank
            FROM ev_telemetry
            WHERE year='{year}' AND month='{month}' AND day='{day}'
        )
        WHERE row_rank = 1
    """

    print("📊 Querying Silver for latest vehicle states...")
    vehicles = run_athena_query(sql)
    print(f"✅ Retrieved {len(vehicles)} vehicle records")

    if not vehicles:
        print("⚠️ No data found for today — skipping Gold write")
        return {"statusCode": 200, "body": "No data"}

    # ── Step 2: Compute Gold metrics per vehicle ──────────────
    fleet_snapshot = []
    total_grid_load = 0.0

    for v in vehicles:
        try:
            soc                  = float(v.get("state_of_charge") or 0)
            battery_temp         = float(v.get("battery_temp_c") or 25)
            charger_kw           = float(v.get("charger_kw") or 7.0)
            time_to_departure    = float(v.get("time_to_departure_min") or 999)
            tariff_period        = v.get("tariff_period", "standard")
            charger_connected    = v.get("charger_connected", "false") == "true"

            # Use assigned charger or default to fast
            effective_charger_kw = charger_kw if charger_kw > 0 else 22.0

            # Compute all 5 Gold metrics
            energy_gap   = compute_energy_gap(soc, effective_charger_kw)
            urgency      = compute_urgency_score(soc, time_to_departure, effective_charger_kw)
            tariff_risk  = compute_tariff_risk(tariff_period, urgency["urgency_score"])
            thermal      = compute_thermal_health(battery_temp)

            # Grid saturation — sum active charger loads
            if charger_connected:
                total_grid_load += charger_kw

            # Optimizer recommendation
            if not thermal["safe_to_charge"]:
                recommendation = "DO_NOT_CHARGE"
                reason         = "Battery temperature critical"
            elif tariff_risk["should_defer"]:
                recommendation = "DEFER_CHARGING"
                reason         = tariff_risk["defer_reason"]
            elif urgency["urgency_level"] in ("critical", "high"):
                recommendation = "CHARGE_NOW"
                reason         = f"Urgency {urgency['urgency_score']} — departure in {time_to_departure}min"
            elif charger_connected and urgency["urgency_level"] == "medium":
                recommendation = "CONTINUE_CHARGING"
                reason         = "Moderate urgency — continue current charge"
            else:
                recommendation = "STANDBY"
                reason         = "SOC healthy, low urgency"

            fleet_snapshot.append({
                "vehicle_id":           v.get("vehicle_id"),
                "snapshot_time":        now.isoformat() + "Z",
                "date":                 today,
                # Current state
                "state_of_charge":      soc,
                "status":               v.get("status"),
                "charger_connected":    charger_connected,
                "charger_kw":           charger_kw,
                "battery_temp_c":       battery_temp,
                "tariff_period":        tariff_period,
                "time_to_departure_min":time_to_departure,
                "next_departure":       v.get("next_departure"),
                # Gold metrics
                "energy_gap":           energy_gap,
                "urgency":              urgency,
                "tariff_risk":          tariff_risk,
                "thermal_health":       thermal,
                # Optimizer output
                "recommendation":       recommendation,
                "reason":               reason,
            })

        except Exception as e:
            print(f"❌ Failed to process {v.get('vehicle_id')}: {str(e)}")
            continue

    # ── Step 3: Fleet-level summary ───────────────────────────
    grid_utilization = round((total_grid_load / GRID_CAPACITY_KW) * 100, 1)

    gold_output = {
        "snapshot_time":     now.isoformat() + "Z",
        "date":              today,
        "fleet_summary": {
            "total_vehicles":       len(fleet_snapshot),
            "vehicles_charging":    sum(1 for v in fleet_snapshot if v["charger_connected"]),
            "vehicles_en_route":    sum(1 for v in fleet_snapshot if v["status"] == "en_route"),
            "vehicles_idle":        sum(1 for v in fleet_snapshot if v["status"] == "at_depot_idle"),
            "avg_soc":              round(sum(v["state_of_charge"] for v in fleet_snapshot) / len(fleet_snapshot), 1),
            "total_grid_load_kw":   round(total_grid_load, 1),
            "grid_utilization_pct": grid_utilization,
            "grid_status":          "critical" if grid_utilization >= 90
                                    else "warning" if grid_utilization >= 70
                                    else "healthy",
            "tariff_period":        fleet_snapshot[0]["tariff_period"] if fleet_snapshot else "unknown",
            "critical_vehicles":    [v["vehicle_id"] for v in fleet_snapshot
                                     if v["urgency"]["urgency_level"] == "critical"],
            "defer_vehicles":       [v["vehicle_id"] for v in fleet_snapshot
                                     if v["recommendation"] == "DEFER_CHARGING"],
        },
        "vehicles": fleet_snapshot
    }

    # ── Step 4: Write to Gold S3 ──────────────────────────────
    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key="fleet_snapshot/latest.json",
        Body=json.dumps(gold_output, indent=2),
        ContentType="application/json"
    )

    # Also archive a timestamped copy for historical analysis
    archive_key = f"fleet_snapshot/archive/{today}/{now.strftime('%H-%M-%S')}.json"
    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key=archive_key,
        Body=json.dumps(gold_output, indent=2),
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
            "snapshot_key":       "fleet_snapshot/latest.json"
        })
    }