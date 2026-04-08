import boto3
import json
import os
import datetime
from decimal import Decimal

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
dynamodb         = boto3.resource('dynamodb', region_name='eu-west-1')
s3_client        = boto3.client('s3', region_name='eu-west-1')

FLEET_TABLE          = os.environ['DYNAMODB_TABLE_NAME']
GOLD_BUCKET          = os.environ['GOLD_BUCKET_NAME']
GRID_CAPACITY_KW     = 150.0
BATTERY_CAPACITY_KWH = 120.0

# Tariff peak hours (UTC) — depot capacity drops 80% during peak
# Only critical/emergency vehicles charge during this window
PEAK_HOURS_UTC = [(7, 10), (17, 21)]   # (start_inclusive, end_exclusive)

# Thermal thresholds
THERMAL_CRITICAL_C  = 45.0   # DO_NOT_CHARGE
THERMAL_WARNING_C   = 38.0   # Throttle to slow charge max

CHARGER_KW = {"slow": 7.0, "fast": 22.0, "rapid": 50.0, "none": 22.0}


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def _is_peak_hour() -> bool:
    hour = datetime.datetime.now(datetime.timezone.utc).hour
    return any(start <= hour < end for start, end in PEAK_HOURS_UTC)


def _effective_grid_capacity() -> float:
    """
    Tariff evasion: drop depot capacity by 80% during peak hours.
    Only vehicles with urgency_score >= 80 (critical) will have
    enough urgency to break through the reduced capacity.
    """
    if _is_peak_hour():
        reduced = GRID_CAPACITY_KW * 0.20
        print(f"⚠️  PEAK TARIFF ACTIVE — grid capacity reduced to "
              f"{reduced}kW ({GRID_CAPACITY_KW}kW × 20%)")
        return reduced
    return GRID_CAPACITY_KW


def _compute_urgency(vehicle: dict) -> tuple[float, str]:
    """
    Returns (urgency_score, urgency_level).
    Identical formula to gold_aggregator for consistency.
    """
    soc               = float(vehicle.get("state_of_charge", 0))
    time_to_departure = float(vehicle.get("time_to_departure_min", 999))
    charger_kw        = float(vehicle.get("charger_kw", 0))
    effective_kw      = charger_kw if charger_kw > 0 else 22.0

    target_soc      = _compute_target_soc(vehicle)
    soc_gap         = max(0.0, target_soc - soc)
    kwh_needed      = (soc_gap / 100.0) * BATTERY_CAPACITY_KWH
    minutes_to_full = (kwh_needed / effective_kw * 60) if effective_kw > 0 else 999

    if time_to_departure <= 0:
        time_pressure = 100.0
    elif minutes_to_full >= time_to_departure:
        time_pressure = 100.0
    else:
        time_pressure = (minutes_to_full / time_to_departure) * 100

    soc_weight    = (soc_gap / target_soc) * 100 if target_soc > 0 else 0
    urgency_score = round(min(100.0, (time_pressure * 0.6) + (soc_weight * 0.4)), 1)
    urgency_level = (
        "critical" if urgency_score >= 80 else
        "high"     if urgency_score >= 60 else
        "medium"   if urgency_score >= 40 else
        "low"
    )
    return urgency_score, urgency_level


def _compute_target_soc(vehicle: dict) -> float:
    """
    Dynamic Target SOC — route-aware charging target.

    Dumb systems charge to 100% every cycle, degrading battery health.
    We set the target based on tomorrow's route energy requirement
    plus a safety buffer.

    Route energy = consumption_rate (kWh/km) × estimated_route_km
    Target SOC   = (route_energy / battery_capacity) × 100 + safety_buffer

    If route distance is unknown, fall back to 95% (conservative default).
    """
    estimated_route_km  = float(vehicle.get("route_km", 27.0))
    consumption_rate    = 1.5    # kWh/km — matches simulator physics
    safety_buffer_pct   = 15.0  # Always keep 15% reserve

    route_energy_kwh    = consumption_rate * estimated_route_km
    route_soc_required  = (route_energy_kwh / BATTERY_CAPACITY_KWH) * 100
    target_soc          = round(min(95.0, route_soc_required + safety_buffer_pct), 1)

    # Floor at 50% — never set a target so low the vehicle can't handle unexpected detours
    return max(50.0, target_soc)


def _thermal_max_kw(battery_temp: float, max_charger_kw: float) -> float:
    """
    Thermal throttling — hardware intelligence.

    Fast charging generates heat, degrading battery cells over time.
    If a vehicle is running hot, we drop its allocated power
    to let the battery cool, and redistribute freed headroom
    to cooler vehicles in the DLM pass.
    """
    if battery_temp >= THERMAL_CRITICAL_C:
        return 0.0   # DO_NOT_CHARGE — thermal protection overrides everything
    elif battery_temp >= THERMAL_WARNING_C:
        # Throttle to 50% of assigned charger max — let it cool
        throttled = round(max_charger_kw * 0.50, 1)
        print(f"🌡️  Thermal throttle | temp {battery_temp}°C → "
              f"capped at {throttled}kW (50% of {max_charger_kw}kW)")
        return throttled
    return max_charger_kw


# ─────────────────────────────────────────────────────────────
# DLM — DYNAMIC LOAD MANAGEMENT PASS
# ─────────────────────────────────────────────────────────────
def dlm_allocate(vehicles: list) -> dict:
    """
    Distributes available grid capacity across charging vehicles
    proportionally by urgency score.

    Returns: {vehicle_id: allocated_kw}

    Algorithm:
    1. Filter to vehicles physically connected to a charger
    2. Apply thermal caps per vehicle (may reduce or zero out)
    3. Rank by urgency score descending (time pressure + SOC gap)
    4. Distribute grid budget proportionally by urgency weight
    5. Clamp each allocation to vehicle's hardware ceiling
    6. Any headroom freed by thermal throttling is redistributed

    This ensures:
    - High urgency vehicles (departing soon, low SOC) get priority power
    - Overheating vehicles are throttled without wasting their headroom
    - Total depot load never exceeds effective grid capacity
    """
    effective_capacity = _effective_grid_capacity()

    # Step 1: Filter to physically connected vehicles
    # charger_connected is stored as DynamoDB BOOL — compare to Python True
    connected = [
        v for v in vehicles
        if v.get("charger_connected") == True
    ]

    if not connected:
        print("⚡ DLM | No vehicles connected — nothing to allocate")
        return {}

    print(f"⚡ DLM | {len(connected)} vehicle(s) connected | "
          f"Grid budget: {effective_capacity}kW")

    # Step 2: Compute urgency scores and thermal caps per vehicle
    vehicle_data = []
    for v in connected:
        vid          = v.get("vehicle_id")
        battery_temp = float(v.get("battery_temp_c", 25))
        charger_type = v.get("charger_type", "fast")
        max_hw_kw    = float(v.get("max_charger_kw",
                             CHARGER_KW.get(charger_type, 22.0)))

        urgency_score, urgency_level = _compute_urgency(v)
        thermal_max = _thermal_max_kw(battery_temp, max_hw_kw)

        vehicle_data.append({
            "vehicle_id":    vid,
            "urgency_score": urgency_score,
            "urgency_level": urgency_level,
            "thermal_max":   thermal_max,
            "max_hw_kw":     max_hw_kw,
            "battery_temp":  battery_temp,
        })

    # Step 3: Rank by urgency score descending
    vehicle_data.sort(key=lambda x: x["urgency_score"], reverse=True)

    # Step 4: Proportional distribution by urgency weight
    # Vehicles with thermal_max = 0 (critical temp) get 0kW regardless
    eligible = [v for v in vehicle_data if v["thermal_max"] > 0]
    total_urgency = sum(v["urgency_score"] for v in eligible)

    allocations = {}
    remaining_budget = effective_capacity

    if total_urgency == 0:
        # All vehicles have zero urgency — equal split among eligible
        equal_share = effective_capacity / len(eligible) if eligible else 0
        for v in eligible:
            alloc = round(min(equal_share, v["thermal_max"]), 2)
            allocations[v["vehicle_id"]] = alloc
    else:
        # Proportional allocation — two passes to handle clamping correctly
        # Pass 1: Compute proportional shares
        raw_allocations = {}
        for v in eligible:
            weight = v["urgency_score"] / total_urgency
            raw_kw = effective_capacity * weight
            # Clamp to thermal max and hardware ceiling
            clamped_kw = round(min(raw_kw, v["thermal_max"], v["max_hw_kw"]), 2)
            raw_allocations[v["vehicle_id"]] = {
                "clamped_kw": clamped_kw,
                "raw_kw":     raw_kw,
                "vehicle":    v,
            }

        # Pass 2: Redistribute headroom freed by clamping
        # If a vehicle was capped below its proportional share,
        # that freed power goes to the next highest urgency vehicle
        allocated_so_far = sum(d["clamped_kw"] for d in raw_allocations.values())
        headroom = round(remaining_budget - allocated_so_far, 2)

        for v in eligible:   # Already sorted by urgency desc
            vid   = v["vehicle_id"]
            entry = raw_allocations[vid]

            if headroom > 0 and entry["clamped_kw"] < entry["raw_kw"]:
                can_absorb = min(
                    headroom,
                    v["thermal_max"] - entry["clamped_kw"],
                    v["max_hw_kw"]   - entry["clamped_kw"]
                )
                if can_absorb > 0:
                    entry["clamped_kw"] = round(entry["clamped_kw"] + can_absorb, 2)
                    headroom            = round(headroom - can_absorb, 2)

            allocations[vid] = entry["clamped_kw"]

    # Zero out thermally blocked vehicles
    for v in vehicle_data:
        if v["thermal_max"] == 0:
            allocations[v["vehicle_id"]] = 0.0
            print(f"🌡️  {v['vehicle_id']} | DO_NOT_CHARGE | "
                  f"temp {v['battery_temp']}°C >= {THERMAL_CRITICAL_C}°C")

    # Log DLM allocation summary
    total_allocated = sum(allocations.values())
    print(f"⚡ DLM allocation | "
          f"Total: {round(total_allocated, 1)}kW / {effective_capacity}kW | "
          f"Utilisation: {round(total_allocated/effective_capacity*100, 1)}%")
    for v in vehicle_data:
        vid   = v["vehicle_id"]
        alloc = allocations.get(vid, 0.0)
        print(f"   {vid:8} | urgency: {v['urgency_score']:5.1f} "
              f"({v['urgency_level']:8}) | "
              f"temp: {v['battery_temp']}°C | "
              f"allocated: {alloc}kW / {v['max_hw_kw']}kW")

    return allocations


# ─────────────────────────────────────────────────────────────
# DECISION ENGINE
# ─────────────────────────────────────────────────────────────
def make_decision(vehicle: dict, current_grid_load: float,
                  allocated_kw: float | None) -> dict:
    vehicle_id        = vehicle.get("vehicle_id")
    soc               = float(vehicle.get("state_of_charge", 0))
    status            = vehicle.get("status", "unknown")
    charger_connected = bool(vehicle.get("charger_connected", False))
    battery_temp      = float(vehicle.get("battery_temp_c", 25))
    charger_kw        = float(vehicle.get("charger_kw", 0))
    time_to_departure = float(vehicle.get("time_to_departure_min", 999))
    tariff_period     = vehicle.get("tariff_period", "standard")

    effective_charger_kw = charger_kw if charger_kw > 0 else 22.0
    target_soc           = _compute_target_soc(vehicle)

    # IN-TRANSIT SHORT-CIRCUIT
    if status in ("en_route", "returning") and not charger_connected:
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "STANDBY",
            "reason":          f"Vehicle in transit — SOC {soc}%",
            "urgency_score":   0.0,
            "urgency_level":   "low",
            "soc":             soc,
            "target_soc":      target_soc,
            "minutes_to_full": 0.0,
            "grid_impact_kw":  0.0,
            "allocated_kw":    0.0,
        }

    # ── Urgency ───────────────────────────────────────────────
    urgency_score, urgency_level = _compute_urgency(vehicle)

    soc_gap         = max(0.0, target_soc - soc)
    kwh_needed      = (soc_gap / 100.0) * BATTERY_CAPACITY_KWH
    minutes_to_full = (kwh_needed / effective_charger_kw * 60) if effective_charger_kw > 0 else 999

    # ── Tariff ────────────────────────────────────────────────
    tariff_cost  = {"peak": 3, "standard": 2, "off_peak": 1}.get(tariff_period, 2)
    should_defer = tariff_cost == 3 and urgency_score < 40

    # ── Grid headroom ─────────────────────────────────────────
    headroom_kw       = _effective_grid_capacity() - current_grid_load
    grid_has_capacity = headroom_kw >= effective_charger_kw

    # ── Resolve actual kW from DLM ────────────────────────────
    # allocated_kw comes from the DLM pass — what this vehicle actually gets
    # None means vehicle is not connected (DLM didn't process it)
    actual_kw = allocated_kw if allocated_kw is not None else 0.0

    # ─────────────────────────────────────────────────────────
    # DECISION TREE
    # ─────────────────────────────────────────────────────────

    # 1. THERMAL SAFETY
    if battery_temp >= THERMAL_CRITICAL_C:
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "DO_NOT_CHARGE",
            "reason":          f"Battery temp critical: {battery_temp}°C — charging suspended",
            "urgency_score":   urgency_score,
            "urgency_level":   urgency_level,
            "soc":             soc,
            "target_soc":      target_soc,
            "minutes_to_full": round(minutes_to_full, 1),
            "grid_impact_kw":  0.0,
            "allocated_kw":    0.0,
        }

    # 2. EMERGENCY RETURN
    if status == "en_route" and soc <= 10.0:
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "EMERGENCY_RETURN",
            "reason":          f"Critical SOC {soc}% while en route — return immediately",
            "urgency_score":   100.0,
            "urgency_level":   "critical",
            "soc":             soc,
            "target_soc":      target_soc,
            "minutes_to_full": round(minutes_to_full, 1),
            "grid_impact_kw":  0.0,
            "allocated_kw":    0.0,
        }

    # 3. CHARGE NOW — high urgency, grid has capacity
    # FIX: Removed `actual_kw > 0` condition — unconnected vehicles never
    # receive DLM allocations so actual_kw is always 0.0 here, making
    # CHARGE_NOW dead code. The recommendation tells the fleet manager
    # to connect; allocated_kw flows on the NEXT optimizer run once connected.
    if urgency_level in ("critical", "high") and not charger_connected:
        if grid_has_capacity:
            return {
                "vehicle_id":      vehicle_id,
                "recommendation":  "CHARGE_NOW",
                "reason":          f"Urgency {urgency_score} — departs in "
                                   f"{time_to_departure}min — SOC {soc}%",
                "urgency_score":   urgency_score,
                "urgency_level":   urgency_level,
                "soc":             soc,
                "target_soc":      target_soc,
                "minutes_to_full": round(minutes_to_full, 1),
                "grid_impact_kw":  effective_charger_kw,
                "allocated_kw":    0.0,  # Will be set by DLM on next run once connected
            }
        else:
            return {
                "vehicle_id":      vehicle_id,
                "recommendation":  "QUEUE_FOR_CHARGING",
                "reason":          f"High urgency but grid at capacity "
                                   f"({current_grid_load}kW / {GRID_CAPACITY_KW}kW)",
                "urgency_score":   urgency_score,
                "urgency_level":   urgency_level,
                "soc":             soc,
                "target_soc":      target_soc,
                "minutes_to_full": round(minutes_to_full, 1),
                "grid_impact_kw":  0.0,
                "allocated_kw":    0.0,
            }

    # 4. CONTINUE CHARGING — already connected, DLM controls rate
    if charger_connected and urgency_level in ("medium", "high", "critical"):
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "CONTINUE_CHARGING",
            "reason":          f"Charging in progress — SOC {soc}% — "
                               f"DLM: {actual_kw}kW — "
                               f"{round(minutes_to_full, 0)}min to {target_soc}%",
            "urgency_score":   urgency_score,
            "urgency_level":   urgency_level,
            "soc":             soc,
            "target_soc":      target_soc,
            "minutes_to_full": round(minutes_to_full, 1),
            "grid_impact_kw":  actual_kw,
            "allocated_kw":    actual_kw,
        }

    # 5. DEFER CHARGING — peak tariff, low urgency
    if should_defer:
        return {
            "vehicle_id":      vehicle_id,
            "recommendation":  "DEFER_CHARGING",
            "reason":          f"Peak tariff — low urgency (score {urgency_score}) "
                               f"— wait for off-peak",
            "urgency_score":   urgency_score,
            "urgency_level":   urgency_level,
            "soc":             soc,
            "target_soc":      target_soc,
            "minutes_to_full": round(minutes_to_full, 1),
            "grid_impact_kw":  0.0,
            "allocated_kw":    0.0,
        }

    # 6. STANDBY — default
    return {
        "vehicle_id":      vehicle_id,
        "recommendation":  "STANDBY",
        "reason":          f"SOC {soc}% — target {target_soc}% — no action required",
        "urgency_score":   urgency_score,
        "urgency_level":   urgency_level,
        "soc":             soc,
        "target_soc":      target_soc,
        "minutes_to_full": round(minutes_to_full, 1),
        "grid_impact_kw":  0.0,
        "allocated_kw":    0.0,
    }


# ─────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────
def handler(event, context):
    now   = datetime.datetime.now(datetime.timezone.utc)
    table = dynamodb.Table(FLEET_TABLE)

    print(f"⚡ Optimizer running | {now.isoformat()} | "
          f"Peak: {_is_peak_hour()}")

    # ── Step 1: Read all vehicles from DynamoDB ───────────────
    response = table.scan()
    vehicles = response.get("Items", [])

    if not vehicles:
        print("⚠️  No vehicles in DynamoDB — skipping")
        return {"statusCode": 200, "body": "No vehicles"}

    print(f"📡 Read {len(vehicles)} vehicles from DynamoDB")

    # ── Step 2: DLM pass — allocate kW before decisions ───────
    # This runs FIRST so every make_decision() call has allocated_kw
    dlm_allocations = dlm_allocate(vehicles)

    # ── Step 3: Calculate current grid load from DLM output ───
    # Use DLM allocations as ground truth — not raw charger_kw from telemetry
    current_grid_load = sum(dlm_allocations.values())

    print(f"⚡ DLM grid load: {round(current_grid_load, 1)}kW / "
          f"{_effective_grid_capacity()}kW")

    # ── Step 4: Run decision engine per vehicle ───────────────
    vehicle_map    = {v["vehicle_id"]: v for v in vehicles}
    schedule       = []
    critical_count = 0
    defer_count    = 0

    for vehicle in vehicles:
        vid       = vehicle.get("vehicle_id")
        alloc_kw  = dlm_allocations.get(vid)   # None if not connected
        decision  = make_decision(vehicle, current_grid_load, alloc_kw)
        schedule.append(decision)

        if decision["urgency_level"] == "critical":
            critical_count += 1
        if decision["recommendation"] == "DEFER_CHARGING":
            defer_count += 1

        print(f"  {decision['vehicle_id']:8} | "
              f"SOC: {decision['soc']:5.1f}% → {decision['target_soc']}% | "
              f"DLM: {decision['allocated_kw']}kW | "
              f"{decision['recommendation']:20} | "
              f"{decision['reason'][:55]}")

    # ── Step 5: Write schedule back to DynamoDB ───────────────
    with table.batch_writer() as batch:
        for decision in schedule:
            original = vehicle_map.get(decision["vehicle_id"], {})
            batch.put_item(Item={
                **original,
                "optimizer_recommendation": decision["recommendation"],
                "optimizer_reason":         decision["reason"],
                "optimizer_urgency_score":  Decimal(str(decision["urgency_score"])),
                "optimizer_urgency_level":  decision["urgency_level"],
                "optimizer_run_at":         now.isoformat() + "Z",
                # DLM fields — fleet_manager reads allocated_kw to control charger_kw
                "allocated_kw":             Decimal(str(decision["allocated_kw"])),
                "target_soc":               Decimal(str(decision["target_soc"])),
            })

    # ── Step 6: Write schedule to Gold S3 ────────────────────
    final_grid_load = sum(
        d["allocated_kw"] for d in schedule
        if d["recommendation"] in ("CHARGE_NOW", "CONTINUE_CHARGING")
    )

    optimizer_output = {
        "optimizer_run_at":      now.isoformat() + "Z",
        "peak_tariff_active":    _is_peak_hour(),
        "grid_capacity_kw":      GRID_CAPACITY_KW,
        "effective_capacity_kw": _effective_grid_capacity(),
        "grid_load_kw":          round(final_grid_load, 1),
        "grid_utilization_pct":  round(
            final_grid_load / _effective_grid_capacity() * 100, 1
        ),
        "critical_vehicles":     critical_count,
        "deferred_vehicles":     defer_count,
        "schedule":              schedule
    }

    s3_client.put_object(
        Bucket=GOLD_BUCKET,
        Key="optimizer/latest_schedule.json",
        Body=json.dumps(optimizer_output, indent=2),
        ContentType="application/json"
    )

    print(f"\n✅ Optimizer complete | "
          f"Critical: {critical_count} | "
          f"Deferred: {defer_count} | "
          f"Grid: {round(final_grid_load/GRID_CAPACITY_KW*100, 1)}% | "
          f"Peak: {_is_peak_hour()}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "vehicles_scheduled":  len(schedule),
            "critical_vehicles":   critical_count,
            "deferred_vehicles":   defer_count,
            "grid_utilization":    round(
                final_grid_load / _effective_grid_capacity() * 100, 1
            ),
            "peak_tariff_active":  _is_peak_hour(),
        })
    }