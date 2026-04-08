import asyncio
import boto3
from boto3.dynamodb.conditions import Attr
import time
import json
import random
import datetime
import sys

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
SQS_QUEUE_URL        = "https://sqs.eu-west-1.amazonaws.com/202564564111/ev-fleet-telemetry-queue"
BATTERY_CAPACITY_KWH = 120.0
DEPOT_LAT            = -26.2041
DEPOT_LONG           =  28.0473
DEPOT_RADIUS_DEG     = 0.005
SIM_TIME_MULTIPLIER  = 600
NUM_VEHICLES         = 10

# DLM: How often to sync allocated_kw from DynamoDB (every N ticks per bus)
DLM_SYNC_TICKS       = 5

# Safe-start: if no DLM allocation received within this many real seconds,
# charge at SAFE_START_KW to prevent buses sitting empty at departure
# 300 real seconds = 5 minutes
SAFE_START_TIMEOUT_S = 300
SAFE_START_KW        = 7.0   # Slow charger rate — safe floor

# Optimizer Lambda invoke: every N simulated minutes
# 300 sim-minutes = 30 real seconds at 600x multiplier
OPTIMIZER_INVOKE_INTERVAL_SIM_MIN = 300

FLEET_TABLE          = "ev-fleet-fleet-state"
OPTIMIZER_FUNCTION   = "ev-fleet-optimizer"

CHARGER_TYPES = {"slow": 7.0, "fast": 22.0, "rapid": 50.0}

ROUTE_WAYPOINTS = [
    (-26.2041,  28.0473),
    (-26.1952,  28.0337),
    (-26.1789,  28.0612),
    (-26.1634,  28.0891),
    (-26.1823,  28.1102),
    (-26.2134,  28.0998),
    (-26.2341,  28.0712),
    (-26.2198,  28.0501),
    (-26.2041,  28.0473),
]

fleet_stats = {
    "total_payloads_sent":   0,
    "total_errors":          0,
    "vehicles_charging":     0,
    "vehicles_en_route":     0,
    "vehicles_idle":         0,
    "avg_soc":               0.0,
    "optimizer_invocations": 0,
    "dlm_syncs":             0,
}

# ─────────────────────────────────────────────────────────────
# AWS CLIENTS — initialised once, shared across all coroutines
# ─────────────────────────────────────────────────────────────
dynamodb_resource = boto3.resource('dynamodb', region_name='eu-west-1')
lambda_client     = boto3.client('lambda',   region_name='eu-west-1')
fleet_table       = dynamodb_resource.Table(FLEET_TABLE)


# ─────────────────────────────────────────────────────────────
# EV BUS STATE MACHINE
# ─────────────────────────────────────────────────────────────
class EVBus:
    def __init__(self, vehicle_id: str):
        self.vehicle_id        = vehicle_id
        self.soc               = round(random.uniform(40.0, 95.0), 2)
        self.status            = "at_depot_idle"
        self.charger_connected = False
        self.charger_kw        = 0.0
        self.charger_type      = None
        self.max_charger_kw    = 0.0
        self.speed_kmh         = 0
        self.battery_temp_c    = round(random.uniform(22.0, 30.0), 1)
        self.odometer_km       = round(random.uniform(5000, 30000), 1)
        self.waypoint_index    = 0
        self.lat               = DEPOT_LAT
        self.long              = DEPOT_LONG
        self.shift_duration_h  = random.choice([8, 10, 12])
        self.assigned_charger  = random.choice(list(CHARGER_TYPES.keys()))
        self.shift_start_hour  = random.choice([5, 6, 7, 14, 15])

        # DLM state
        # None = no optimizer decision yet (cold start — do not charge)
        self.allocated_kw      = None
        self.tick_count        = 0

        # SAFE-START: track when this bus plugged in (real wall-clock time)
        # If allocated_kw is still None after SAFE_START_TIMEOUT_S real seconds,
        # charge at SAFE_START_KW to prevent buses sitting empty at departure
        self.plug_in_time: float | None = None

        now            = datetime.datetime.now(datetime.timezone.utc)
        departure_type = random.choice(["imminent", "soon", "later"])
        if departure_type == "imminent":
            self.next_departure   = now + datetime.timedelta(minutes=random.randint(1, 5))
            self.shift_start_hour = self.next_departure.hour
        elif departure_type == "soon":
            self.next_departure   = now + datetime.timedelta(minutes=random.randint(10, 30))
            self.shift_start_hour = self.next_departure.hour
        else:
            self.next_departure = self._next_departure_time()

    def _next_departure_time(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        departure = now.replace(
            hour=self.shift_start_hour,
            minute=random.randint(0, 59),
            second=0, microsecond=0
        )
        if departure < now:
            departure += datetime.timedelta(days=1)
        return departure

    def _tariff_period(self):
        hour = datetime.datetime.now(datetime.timezone.utc).hour
        if 7 <= hour < 10 or 17 <= hour < 21:
            return "peak"
        elif 22 <= hour or hour < 6:
            return "off_peak"
        else:
            return "standard"

    def _time_to_departure_min(self):
        now      = datetime.datetime.now(datetime.timezone.utc)
        next_dep = self.next_departure
        if next_dep.tzinfo is None:
            next_dep = next_dep.replace(tzinfo=datetime.timezone.utc)
        return round(max((next_dep - now).total_seconds() / 60, 0), 1)

    def _at_depot(self):
        return (
            abs(self.lat  - DEPOT_LAT)  < DEPOT_RADIUS_DEG and
            abs(self.long - DEPOT_LONG) < DEPOT_RADIUS_DEG
        )

    def _should_depart(self):
        return (
            self.status == "at_depot_idle" and
            self._time_to_departure_min() <= 5 and
            self.soc >= 30.0
        )

    def _should_return(self):
        return (
            self.status == "en_route" and
            (self.soc <= 20.0 or self.waypoint_index >= len(ROUTE_WAYPOINTS) - 1)
        )

    def _should_start_charging(self):
        return self.status == "returning" and self._at_depot()

    def _should_stop_charging(self):
        return self.status == "at_depot_charging" and self.soc >= 98.0

    def apply_dlm_allocation(self, allocated_kw: float | None):
        """
        Called by the fleet manager after a DynamoDB sync.
        Sets the actual charging rate the bus will use this tick.

        None  → cold start, no optimizer decision yet → use safe-start logic
        0.0   → optimizer explicitly said DO_NOT_CHARGE or DEFER
        >0.0  → optimizer allocated this many kW to this vehicle
        """
        self.allocated_kw = allocated_kw

    def check_safety(self) -> float:
        """
        SAFETY INTERLOCK — never trust the optimizer blindly.

        Hard overrides that take effect regardless of what the optimizer says:
        1. Thermal cutoff  — battery too hot, force 0kW
        2. SOC ceiling     — battery full, force 0kW
        3. Safe-start      — no allocation received within timeout, use floor rate
        4. Normal          — return optimizer allocation

        This is the final gate before electrons flow.
        Called every tick in at_depot_charging physics.
        """
        # 1. Thermal cutoff — always first, non-negotiable
        if self.battery_temp_c >= 45.0:
            return 0.0

        # 2. SOC ceiling — battery is full
        if self.soc >= 98.0:
            return 0.0

        # 3. Optimizer explicitly said stop
        if self.allocated_kw == 0.0:
            return 0.0

        # 4. Safe-start fallback — optimizer hasn't responded yet
        # If we've been waiting longer than SAFE_START_TIMEOUT_S real seconds
        # and still have no allocation, charge at the safe floor rate
        # to ensure the bus isn't empty at departure
        if self.allocated_kw is None:
            if self.plug_in_time is not None:
                elapsed = time.time() - self.plug_in_time
                if elapsed >= SAFE_START_TIMEOUT_S:
                    print(f"⚠️  {self.vehicle_id} | SAFE-START | "
                          f"No DLM allocation after {elapsed:.0f}s — "
                          f"charging at floor rate {SAFE_START_KW}kW")
                    return SAFE_START_KW
            # Still within timeout window — wait
            return 0.0

        # 5. Normal — return optimizer allocation capped at hardware ceiling
        return round(min(float(self.allocated_kw), self.max_charger_kw), 2)

    def _effective_charge_rate(self, raw_kw: float) -> float:
        """
        LITHIUM-ION TAPER — charging curve simulation.

        Real Li-ion batteries don't charge linearly. From 0-80% SOC they
        accept full power (CC — constant current phase). Above 80% the BMS
        reduces current to protect cell chemistry and extend battery life
        (CV — constant voltage phase). Charging at full rate above 80%
        degrades the battery over time.

        This tapers the effective charge rate based on SOC regardless of
        what the DLM allocated — the battery management system enforces
        this in hardware on real vehicles.

              0% ────────── 80%  full rate
             80% ────────── 95%  linear taper to 20% of full rate
             95% ────────── 98%  trickle only (3kW max)
             98%+           stop charging (check_safety handles this)
        """
        if self.soc >= 95.0:
            # Trickle — protect cells near full
            return min(raw_kw, 3.0)
        elif self.soc >= 80.0:
            # Linear taper: 100% power at 80% SOC → 20% power at 95% SOC
            taper_factor = 1.0 - ((self.soc - 80.0) / 15.0) * 0.80
            return round(raw_kw * taper_factor, 2)
        else:
            # Full CC phase — no taper below 80%
            return raw_kw

    def update(self, real_interval_s: float) -> None:
        sim_interval_h = (real_interval_s * SIM_TIME_MULTIPLIER) / 3600
        self.tick_count += 1

        # ── State transitions ─────────────────────────────────
        if self._should_depart():
            self.status            = "en_route"
            self.charger_connected = False
            self.charger_kw        = 0.0
            self.charger_type      = None
            self.max_charger_kw    = 0.0
            self.allocated_kw      = None
            self.plug_in_time      = None   # Reset safe-start timer on departure
            self.waypoint_index    = 0
            self.next_departure    = self._next_departure_time()
            print(f"🚌 {self.vehicle_id} departed | SOC: {self.soc:.1f}%")

        elif self._should_return():
            self.status         = "returning"
            self.waypoint_index = len(ROUTE_WAYPOINTS) - 1
            print(f"↩️  {self.vehicle_id} returning | SOC: {self.soc:.1f}%")

        elif self._should_start_charging():
            self.status            = "at_depot_charging"
            self.charger_connected = True
            self.charger_type      = self.assigned_charger
            self.max_charger_kw    = CHARGER_TYPES[self.charger_type]
            self.charger_kw        = 0.0
            self.plug_in_time      = time.time()  # Start safe-start countdown
            print(f"🔌 {self.vehicle_id} connected | {self.charger_type} "
                  f"(max {self.max_charger_kw}kW) | awaiting DLM allocation...")

        elif self._should_stop_charging():
            self.status            = "at_depot_idle"
            self.charger_connected = False
            self.charger_kw        = 0.0
            self.charger_type      = None
            self.max_charger_kw    = 0.0
            self.allocated_kw      = None
            self.plug_in_time      = None   # Reset safe-start timer
            self.speed_kmh         = 0
            print(f"✅ {self.vehicle_id} fully charged | SOC: {self.soc:.1f}%")

        # ── Physics ───────────────────────────────────────────
        if self.status == "en_route":
            self.speed_kmh      = random.randint(30, 80)
            km                  = self.speed_kmh * sim_interval_h
            self.soc            = round(max(0.0, self.soc - (km * 1.5 / BATTERY_CAPACITY_KWH) * 100), 2)
            self.odometer_km    = round(self.odometer_km + km, 1)
            self.waypoint_index = min(self.waypoint_index + 1, len(ROUTE_WAYPOINTS) - 1)
            wp                  = ROUTE_WAYPOINTS[self.waypoint_index]
            self.lat            = round(wp[0] + random.uniform(-0.001, 0.001), 6)
            self.long           = round(wp[1] + random.uniform(-0.001, 0.001), 6)
            self.battery_temp_c = round(min(45.0, self.battery_temp_c + random.uniform(0.1, 0.5)), 1)

        elif self.status == "returning":
            self.speed_kmh      = random.randint(20, 50)
            km                  = self.speed_kmh * sim_interval_h
            self.soc            = round(max(0.0, self.soc - (km * 1.5 / BATTERY_CAPACITY_KWH) * 100), 2)
            self.odometer_km    = round(self.odometer_km + km, 1)
            self.lat            = round(DEPOT_LAT  + random.uniform(-0.003, 0.003), 6)
            self.long           = round(DEPOT_LONG + random.uniform(-0.003, 0.003), 6)
            if random.random() < 0.4:
                self.lat  = round(DEPOT_LAT  + random.uniform(-0.001, 0.001), 6)
                self.long = round(DEPOT_LONG + random.uniform(-0.001, 0.001), 6)

        elif self.status == "at_depot_charging":
            self.speed_kmh = 0

            # ── DLM POWER CONTROL ─────────────────────────────
            # Step 1: check_safety() — hard overrides (thermal, full, safe-start)
            # Step 2: _effective_charge_rate() — Li-ion taper above 80% SOC
            # Step 3: Apply to SOC physics
            safe_kw          = self.check_safety()
            self.charger_kw  = self._effective_charge_rate(safe_kw)

            # SOC gain based on actual tapered rate
            self.soc            = round(
                min(100.0, self.soc + (self.charger_kw * sim_interval_h / BATTERY_CAPACITY_KWH) * 100), 2
            )
            self.battery_temp_c = round(
                max(22.0, self.battery_temp_c - random.uniform(0.1, 0.3)), 1
            )

        elif self.status == "at_depot_idle":
            self.speed_kmh      = 0
            self.soc            = round(max(0.0, self.soc - random.uniform(0.01, 0.05) * sim_interval_h), 2)
            self.battery_temp_c = round(max(20.0, self.battery_temp_c - random.uniform(0.05, 0.1)), 1)
            self.lat            = DEPOT_LAT
            self.long           = DEPOT_LONG

    def payload(self) -> dict:
        return {
            "vehicle_id":              self.vehicle_id,
            "timestamp":               time.time(),
            "state_of_charge":         self.soc,
            "status":                  self.status,
            "charger_connected":       self.charger_connected,
            "charger_type":            self.charger_type,
            "charger_kw":              self.charger_kw,
            "max_charger_kw":          self.max_charger_kw,
            "allocated_kw":            self.allocated_kw,
            "battery_temp_c":          self.battery_temp_c,
            "speed_kmh":               self.speed_kmh,
            "odometer_km":             self.odometer_km,
            "next_departure":          self.next_departure.isoformat() + "Z",
            "time_to_departure_min":   self._time_to_departure_min(),
            "tariff_period":           self._tariff_period(),
            "coordinates": {
                "lat":  self.lat,
                "long": self.long
            }
        }


# ─────────────────────────────────────────────────────────────
# DLM SYNC — fleet-level, one DynamoDB call for all buses
# ─────────────────────────────────────────────────────────────
async def sync_dlm_allocations(buses: list, loop):
    """
    Single DynamoDB Scan filtered to charging vehicles.
    Updates allocated_kw on all bus objects in memory.
    One network call — not N calls. Runs every DLM_SYNC_TICKS ticks.
    """
    try:
        response = await loop.run_in_executor(
            None,
            lambda: fleet_table.scan(
                # FIX: charger_connected is stored as DynamoDB BOOL (Python True),
                # not the string "true". Using eq("true") always returns 0 results,
                # which is why allocated_kw was never updating — the allocation_map
                # was always empty.
                FilterExpression=Attr("charger_connected").eq(True)
            )
        )
        items = response.get("Items", [])

        # Build lookup: vehicle_id → allocated_kw
        allocation_map = {}
        for item in items:
            vid   = item.get("vehicle_id")
            alloc = item.get("allocated_kw")
            if vid and alloc is not None:
                allocation_map[vid] = float(alloc)

        # Update all bus objects in memory
        updated = 0
        for bus in buses:
            if bus.status == "at_depot_charging":
                prev      = bus.allocated_kw
                new_alloc = allocation_map.get(bus.vehicle_id)
                bus.apply_dlm_allocation(new_alloc)

                if new_alloc is not None and prev != new_alloc:
                    print(f"⚡ DLM | {bus.vehicle_id} | "
                          f"allocated_kw: {prev} → {new_alloc}kW")
                    updated += 1

        fleet_stats["dlm_syncs"] += 1
        if updated > 0:
            print(f"🔄 DLM sync | {updated} allocation(s) updated | "
                  f"Total syncs: {fleet_stats['dlm_syncs']}")

    except Exception as e:
        print(f"⚠️  DLM sync failed: {e}")
        # Non-fatal — buses keep last known allocation


# ─────────────────────────────────────────────────────────────
# OPTIMIZER INVOKE
# ─────────────────────────────────────────────────────────────
async def invoke_optimizer(loop):
    """
    Explicitly invokes the optimizer Lambda.
    Called every OPTIMIZER_INVOKE_INTERVAL_SIM_MIN simulated minutes.

    At 600x multiplier: 300 sim-minutes = 30 real seconds.
    Keeps the optimizer in sync with simulated time instead of
    relying on EventBridge's real-world 5-minute schedule.
    """
    try:
        await loop.run_in_executor(
            None,
            lambda: lambda_client.invoke(
                FunctionName=OPTIMIZER_FUNCTION,
                InvocationType="Event",   # Async — don't block the sim loop
                Payload=json.dumps({})
            )
        )
        fleet_stats["optimizer_invocations"] += 1
        print(f"🧠 Optimizer invoked | "
              f"Total invocations: {fleet_stats['optimizer_invocations']}")

    except Exception as e:
        print(f"⚠️  Optimizer invoke failed: {e}")
        # Non-fatal — optimizer will still run on EventBridge schedule


# ─────────────────────────────────────────────────────────────
# ASYNC BUS RUNNER
# ─────────────────────────────────────────────────────────────
async def run_bus(bus: EVBus, sqs_client, buses: list):
    print(f"🚀 {bus.vehicle_id} online | SOC: {bus.soc:.1f}% | "
          f"Departs in: {bus._time_to_departure_min()}min")

    loop = asyncio.get_event_loop()

    while True:
        real_interval_s = random.uniform(1.0, 3.0)
        bus.update(real_interval_s)

        # ── DLM sync every DLM_SYNC_TICKS ticks ──────────────
        # Only Bus-00 triggers the fleet-wide sync to avoid
        # 10 coroutines all firing the same DynamoDB call simultaneously
        if bus.vehicle_id == "Bus-00" and bus.tick_count % DLM_SYNC_TICKS == 0:
            await sync_dlm_allocations(buses, loop)

        data = bus.payload()

        try:
            await loop.run_in_executor(
                None,
                lambda: sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(data)
                )
            )
            charger_icon = (
                f"🔌 {bus.charger_type} | DLM: {bus.charger_kw}kW"
                if bus.charger_connected else "🚗"
            )
            print(f"✅ {bus.vehicle_id} | SOC: {bus.soc:.1f}% | "
                  f"Status: {bus.status} | "
                  f"Charger: {charger_icon} | "
                  f"Temp: {bus.battery_temp_c}°C | "
                  f"Tariff: {data['tariff_period']} | "
                  f"Departs in: {data['time_to_departure_min']}min")
            fleet_stats["total_payloads_sent"] += 1

        except Exception as e:
            print(f"❌ {bus.vehicle_id} SQS error: {e}")
            fleet_stats["total_errors"] += 1

        await asyncio.sleep(real_interval_s)


# ─────────────────────────────────────────────────────────────
# OPTIMIZER SCHEDULER
# ─────────────────────────────────────────────────────────────
async def optimizer_scheduler():
    """
    Invokes the optimizer Lambda every OPTIMIZER_INVOKE_INTERVAL_SIM_MIN
    simulated minutes.

    Real sleep interval = sim_minutes / SIM_TIME_MULTIPLIER * 60
    300 sim-min / 600 * 60 = 30 real seconds
    """
    real_interval_s = (OPTIMIZER_INVOKE_INTERVAL_SIM_MIN / SIM_TIME_MULTIPLIER) * 60
    loop = asyncio.get_event_loop()

    print(f"🧠 Optimizer scheduler active | "
          f"Invoking every {OPTIMIZER_INVOKE_INTERVAL_SIM_MIN} sim-min "
          f"({real_interval_s:.0f} real seconds)")

    # First invoke immediately so buses aren't waiting on cold start
    await invoke_optimizer(loop)

    while True:
        await asyncio.sleep(real_interval_s)
        await invoke_optimizer(loop)


# ─────────────────────────────────────────────────────────────
# FLEET MONITOR
# ─────────────────────────────────────────────────────────────
async def fleet_monitor(buses: list):
    while True:
        await asyncio.sleep(30)

        soc_values = [b.soc for b in buses]
        fleet_stats["avg_soc"]           = round(sum(soc_values) / len(soc_values), 1)
        fleet_stats["vehicles_charging"] = sum(1 for b in buses if b.status == "at_depot_charging")
        fleet_stats["vehicles_en_route"] = sum(1 for b in buses if b.status in ("en_route", "returning"))
        fleet_stats["vehicles_idle"]     = sum(1 for b in buses if b.status == "at_depot_idle")

        charging_buses  = [b for b in buses if b.status == "at_depot_charging"]
        total_allocated = sum(b.charger_kw for b in charging_buses)
        total_max       = sum(b.max_charger_kw for b in charging_buses)

        print(f"\n{'─'*70}")
        print(f"📊 FLEET REPORT | {datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M:%S')} UTC")
        print(f"   Avg SOC:          {fleet_stats['avg_soc']}%")
        print(f"   Charging:         {fleet_stats['vehicles_charging']} vehicles")
        print(f"   En Route:         {fleet_stats['vehicles_en_route']} vehicles")
        print(f"   Idle at Depot:    {fleet_stats['vehicles_idle']} vehicles")
        print(f"   Total Sent:       {fleet_stats['total_payloads_sent']} payloads")
        print(f"   Total Errors:     {fleet_stats['total_errors']}")
        print(f"   DLM Grid Load:    {total_allocated:.1f}kW / 150kW "
              f"({round(total_allocated/150*100, 1)}%)")
        print(f"   DLM Utilisation:  {total_allocated:.1f}kW allocated "
              f"vs {total_max:.1f}kW max possible")
        print(f"   Optimizer Runs:   {fleet_stats['optimizer_invocations']}")
        print(f"   DLM Syncs:        {fleet_stats['dlm_syncs']}")

        if charging_buses:
            print(f"   ── Per-vehicle DLM ──────────────────────────────")
            for b in charging_buses:
                alloc_str = f"{b.charger_kw}kW" if b.allocated_kw is not None else "waiting..."
                taper_note = " (tapered)" if b.soc >= 80.0 and b.charger_kw > 0 else ""
                print(f"   {b.vehicle_id} | SOC: {b.soc:.1f}% | "
                      f"DLM: {alloc_str}{taper_note} | max: {b.max_charger_kw}kW | "
                      f"temp: {b.battery_temp_c}°C")

        low_soc = [b for b in buses if b.soc < 25.0 and not b.charger_connected]
        if low_soc:
            print(f"   ⚠️  LOW SOC: {', '.join(b.vehicle_id for b in low_soc)}")
        print(f"{'─'*70}\n")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
async def main():
    print(f"{'═'*70}")
    print(f"   EV FLEET CHARGING OPTIMIZER — Fleet Manager v3")
    print(f"   Launching {NUM_VEHICLES} vehicles | Depot: Johannesburg")
    print(f"   Transport:      SQS (guaranteed delivery)")
    print(f"   Sim multiplier: {SIM_TIME_MULTIPLIER}x")
    print(f"   DLM:            Active — optimizer controls charger_kw")
    print(f"   Safe-start:     {SAFE_START_TIMEOUT_S}s timeout → {SAFE_START_KW}kW floor")
    print(f"   Li-ion taper:   Active above 80% SOC")
    print(f"   Optimizer:      Invoked every "
          f"{OPTIMIZER_INVOKE_INTERVAL_SIM_MIN} sim-min "
          f"({OPTIMIZER_INVOKE_INTERVAL_SIM_MIN/SIM_TIME_MULTIPLIER*60:.0f}s real)")
    print(f"{'═'*70}\n")

    sqs_client = boto3.client('sqs', region_name='eu-west-1')
    buses      = [EVBus(f"Bus-{i:02d}") for i in range(NUM_VEHICLES)]

    tasks = [run_bus(bus, sqs_client, buses) for bus in buses]
    tasks.append(fleet_monitor(buses))
    tasks.append(optimizer_scheduler())

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n🛑 Fleet grounded. "
              f"Total payloads sent: {fleet_stats['total_payloads_sent']} | "
              f"Optimizer invocations: {fleet_stats['optimizer_invocations']}")
        sys.exit(0)