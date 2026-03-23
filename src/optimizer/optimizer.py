import boto3
import json
import os
import datetime
from decimal import Decimal  # FIX 1: Vital for DynamoDB numeric storage

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
dynamodb         = boto3.resource('dynamodb', region_name='eu-west-1')
s3_client        = boto3.client('s3', region_name='eu-west-1')

FLEET_TABLE      = os.environ['DYNAMODB_TABLE_NAME']
GOLD_BUCKET      = os.environ['GOLD_BUCKET_NAME']
GRID_CAPACITY_KW = 150.0
BATTERY_CAPACITY_KWH = 120.0

CHARGER_KW = {"slow": 7.0, "fast": 22.0, "rapid": 50.0, "none": 22.0}


# ─────────────────────────────────────────────────────────────
# DECISION ENGINE
# ─────────────────────────────────────────────────────────────
def make_decision(vehicle: dict, current_grid_load: float) -> dict:
    vehicle_id         = vehicle.get("vehicle_id")
    soc                = float(vehicle.get("state_of_charge", 0))
    status             = vehicle.get("status", "unknown")
    charger_connected  = vehicle.get("charger_connected", False)
    if isinstance(charger_connected, str):
        charger_connected = charger_connected.lower() == "true"
    battery_temp       = float(vehicle.get("battery_temp_c", 25))
    charger_kw         = float(vehicle.get("charger_kw", 0))
    time_to_departure  = float(vehicle.get("time_to_departure_min", 999))
    tariff_period      = vehicle.get("tariff_period", "standard")

    effective_charger_kw = charger_kw if charger_kw > 0 else 22.0

    # FIX 2: IN-TRANSIT SHORT-CIRCUIT — if it's moving, it's STANDBY. Period.
    # This prevents CHARGE_NOW ever being issued to a vehicle that isn't at depot.
    if status in ("en_route", "returning") and not charger_connected:
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "STANDBY",
            "reason":          f"Vehicle in transit — SOC {soc}%",
            "urgency_score":   0.0,
            "urgency_level":   "low",
            "soc":             soc,
            "minutes_to_full": 0.0,
            "grid_impact_kw":  0.0,
        }

    # ── Compute urgency ───────────────────────────────────────
    target_soc        = 95.0
    soc_gap           = max(0.0, target_soc - soc)
    kwh_needed        = (soc_gap / 100.0) * BATTERY_CAPACITY_KWH
    minutes_to_full   = (kwh_needed / effective_charger_kw * 60) if effective_charger_kw > 0 else 999

    if time_to_departure <= 0:
        time_pressure = 100.0
    elif minutes_to_full >= time_to_departure:
        time_pressure = 100.0
    else:
        time_pressure = (minutes_to_full / time_to_departure) * 100

    soc_weight    = (soc_gap / target_soc) * 100
    urgency_score = round(min(100.0, (time_pressure * 0.6) + (soc_weight * 0.4)), 1)
    urgency_level = (
        "critical" if urgency_score >= 80 else
        "high"     if urgency_score >= 60 else
        "medium"   if urgency_score >= 40 else
        "low"
    )

    # ── Tariff cost ───────────────────────────────────────────
    tariff_cost   = {"peak": 3, "standard": 2, "off_peak": 1}.get(tariff_period, 2)
    should_defer  = tariff_cost == 3 and urgency_score < 40

    # ── Grid headroom ─────────────────────────────────────────
    headroom_kw        = GRID_CAPACITY_KW - current_grid_load
    grid_has_capacity  = headroom_kw >= effective_charger_kw

    # ─────────────────────────────────────────────────────────
    # DECISION TREE — priority order
    # ─────────────────────────────────────────────────────────

    # 1. SAFETY FIRST — thermal protection
    if battery_temp >= 45.0:
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "DO_NOT_CHARGE",
            "reason":          f"Battery temp critical: {battery_temp}°C — charging suspended",
            "urgency_score":   urgency_score,
            "urgency_level":   urgency_level,
            "soc":             soc,
            "minutes_to_full": round(minutes_to_full, 1),
            "grid_impact_kw":  0.0,
        }

    # 2. EMERGENCY RETURN — critically low SOC while driving
    if status == "en_route" and soc <= 10.0:
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "EMERGENCY_RETURN",
            "reason":          f"Critical SOC {soc}% while en route — return to depot immediately",
            "urgency_score":   100.0,
            "urgency_level":   "critical",
            "soc":             soc,
            "minutes_to_full": round(minutes_to_full, 1),
            "grid_impact_kw":  0.0,
        }

    # 3. CHARGE NOW — high urgency, grid has capacity
    if urgency_level in ("critical", "high") and not charger_connected:
        if grid_has_capacity:
            return {
                "vehicle_id":      vehicle_id,
                "recommendation":  "CHARGE_NOW",
                "reason":          f"Urgency {urgency_score} — departs in {time_to_departure}min — SOC {soc}%",
                "urgency_score":   urgency_score,
                "urgency_level":   urgency_level,
                "soc":             soc,
                "minutes_to_full": round(minutes_to_full, 1),
                "grid_impact_kw":  effective_charger_kw,
            }
        else:
            return {
                "vehicle_id":      vehicle_id,
                "recommendation":  "QUEUE_FOR_CHARGING",
                "reason":          f"High urgency but grid at capacity ({current_grid_load}kW / {GRID_CAPACITY_KW}kW)",
                "urgency_score":   urgency_score,
                "urgency_level":   urgency_level,
                "soc":             soc,
                "minutes_to_full": round(minutes_to_full, 1),
                "grid_impact_kw":  0.0,
            }

    # 4. CONTINUE CHARGING — already plugged in and making progress
    # Intentionally catches critical/high/medium vehicles that are already connected
    if charger_connected and urgency_level in ("medium", "high", "critical"):
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "CONTINUE_CHARGING",
            "reason":          f"Charging in progress — SOC {soc}% — {round(minutes_to_full, 0)}min to full",
            "urgency_score":   urgency_score,
            "urgency_level":   urgency_level,
            "soc":             soc,
            "minutes_to_full": round(minutes_to_full, 1),
            "grid_impact_kw":  charger_kw,
        }

    # 5. DEFER CHARGING — peak tariff, low urgency
    if should_defer:
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "DEFER_CHARGING",
            "reason":          f"Peak tariff — low urgency (score {urgency_score}) — wait for off-peak",
            "urgency_score":   urgency_score,
            "urgency_level":   urgency_level,
            "soc":             soc,
            "minutes_to_full": round(minutes_to_full, 1),
            "grid_impact_kw":  0.0,
        }

    # 6. STANDBY — default
    return {
        "vehicle_id":      vehicle_id,
        "recommendation":  "STANDBY",
        "reason":          f"SOC healthy at {soc}% — no action required",
        "urgency_score":   urgency_score,
        "urgency_level":   urgency_level,
        "soc":             soc,
        "minutes_to_full": round(minutes_to_full, 1),
        "grid_impact_kw":  0.0,
    }


# ─────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────
def handler(event, context):
    now   = datetime.datetime.now(datetime.timezone.utc)
    table = dynamodb.Table(FLEET_TABLE)

    print(f"⚡ Optimizer running | {now.isoformat()}")

    # ── Step 1: Read all vehicles from DynamoDB ───────────────
    response = table.scan()
    vehicles = response.get("Items", [])

    if not vehicles:
        print("⚠️  No vehicles in DynamoDB — skipping")
        return {"statusCode": 200, "body": "No vehicles"}

    print(f"📡 Read {len(vehicles)} vehicles from DynamoDB")

    # ── Step 2: Calculate current grid load ───────────────────
    current_grid_load = sum(
        float(v.get("charger_kw", 0))
        for v in vehicles
        if str(v.get("charger_connected", "false")).lower() == "true"
    )

    print(f"⚡ Current grid load: {current_grid_load}kW / {GRID_CAPACITY_KW}kW "
          f"({round(current_grid_load/GRID_CAPACITY_KW*100, 1)}%)")

    # ── Step 3: Run decision engine per vehicle ───────────────
    # FIX 3: Build vehicle lookup map ONCE before the loop — O(N) not O(N²)
    vehicle_map    = {v["vehicle_id"]: v for v in vehicles}
    schedule       = []
    critical_count = 0
    defer_count    = 0

    for vehicle in vehicles:
        decision = make_decision(vehicle, current_grid_load)
        schedule.append(decision)

        # Update grid load dynamically as we assign chargers
        if decision["recommendation"] == "CHARGE_NOW":
            current_grid_load += decision["grid_impact_kw"]

        if decision["urgency_level"] == "critical":
            critical_count += 1
        if decision["recommendation"] == "DEFER_CHARGING":
            defer_count += 1

        print(f"  {decision['vehicle_id']:8} | "
              f"SOC: {decision['soc']:5.1f}% | "
              f"{decision['recommendation']:20} | "
              f"{decision['reason'][:60]}")

    # ── Step 4: Write schedule back to DynamoDB ───────────────
    with table.batch_writer() as batch:
        for decision in schedule:
            original = vehicle_map.get(decision["vehicle_id"], {})
            batch.put_item(Item={
                **original,
                "optimizer_recommendation": decision["recommendation"],
                "optimizer_reason":         decision["reason"],
                # FIX 1: Decimal conversion — DynamoDB rejects Python floats
                "optimizer_urgency_score":  Decimal(str(decision["urgency_score"])),
                "optimizer_urgency_level":  decision["urgency_level"],
                "optimizer_run_at":         now.isoformat() + "Z",
            })

    # ── Step 5: Write schedule to Gold S3 ────────────────────
    optimizer_output = {
        "optimizer_run_at":     now.isoformat() + "Z",
        "grid_load_kw":         round(current_grid_load, 1),
        "grid_capacity_kw":     GRID_CAPACITY_KW,
        "grid_utilization_pct": round(current_grid_load / GRID_CAPACITY_KW * 100, 1),
        "critical_vehicles":    critical_count,
        "deferred_vehicles":    defer_count,
        "schedule":             schedule
    }

    s3_client.put_object(
        Bucket=GOLD_BUCKET,
        Key="optimizer/latest_schedule.json",
        Body=json.dumps(optimizer_output, indent=2),
        ContentType="application/json"
    )

    print(f"\n✅ Schedule written | Critical: {critical_count} | "
          f"Deferred: {defer_count} | "
          f"Grid: {round(current_grid_load/GRID_CAPACITY_KW*100,1)}%")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "vehicles_scheduled": len(schedule),
            "critical_vehicles":  critical_count,
            "deferred_vehicles":  defer_count,
            "grid_utilization":   round(current_grid_load / GRID_CAPACITY_KW * 100, 1)
        })
    }