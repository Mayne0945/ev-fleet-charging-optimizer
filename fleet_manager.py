import asyncio
import boto3
import time
import json
import random
import datetime
import sys

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
SQS_QUEUE_URL = "https://sqs.eu-west-1.amazonaws.com/202564564111/ev-fleet-telemetry-queue"
BATTERY_CAPACITY_KWH = 120.0
DEPOT_LAT            = -26.2041
DEPOT_LONG           =  28.0473
DEPOT_RADIUS_DEG     = 0.005
SIM_TIME_MULTIPLIER  = 600
NUM_VEHICLES         = 10

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
    "total_payloads_sent": 0,
    "total_errors":        0,
    "vehicles_charging":   0,
    "vehicles_en_route":   0,
    "vehicles_idle":       0,
    "avg_soc":             0.0,
}

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
        self.speed_kmh         = 0
        self.battery_temp_c    = round(random.uniform(22.0, 30.0), 1)
        self.odometer_km       = round(random.uniform(5000, 30000), 1)
        self.waypoint_index    = 0
        self.lat               = DEPOT_LAT
        self.long              = DEPOT_LONG
        self.shift_duration_h  = random.choice([8, 10, 12])
        self.assigned_charger  = random.choice(list(CHARGER_TYPES.keys()))
        self.shift_start_hour  = random.choice([5, 6, 7, 14, 15])

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

    def update(self, real_interval_s: float) -> None:
        sim_interval_h = (real_interval_s * SIM_TIME_MULTIPLIER) / 3600

        if self._should_depart():
            self.status            = "en_route"
            self.charger_connected = False
            self.charger_kw        = 0.0
            self.charger_type      = None
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
            self.charger_kw        = CHARGER_TYPES[self.charger_type]
            print(f"🔌 {self.vehicle_id} charging | {self.charger_type} ({self.charger_kw}kW)")

        elif self._should_stop_charging():
            self.status            = "at_depot_idle"
            self.charger_connected = False
            self.charger_kw        = 0.0
            self.charger_type      = None
            self.speed_kmh         = 0
            print(f"✅ {self.vehicle_id} fully charged | SOC: {self.soc:.1f}%")

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
            self.speed_kmh      = 0
            self.soc            = round(min(100.0, self.soc + (self.charger_kw * sim_interval_h / BATTERY_CAPACITY_KWH) * 100), 2)
            self.battery_temp_c = round(max(22.0, self.battery_temp_c - random.uniform(0.1, 0.3)), 1)

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
# ASYNC BUS RUNNER — sends directly to SQS
# ─────────────────────────────────────────────────────────────
async def run_bus(bus: EVBus, sqs_client):
    print(f"🚀 {bus.vehicle_id} online | SOC: {bus.soc:.1f}% | "
          f"Departs in: {bus._time_to_departure_min()}min")

    loop = asyncio.get_event_loop()

    while True:
        real_interval_s = random.uniform(1.0, 3.0)
        bus.update(real_interval_s)
        data = bus.payload()

        try:
            # Run boto3 SQS send in thread pool to keep async non-blocking
            await loop.run_in_executor(
                None,
                lambda: sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(data)
                )
            )
            charger_icon = (
                f"🔌 {bus.charger_type} ({bus.charger_kw}kW)"
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

        print(f"\n{'─'*70}")
        print(f"📊 FLEET REPORT | {datetime.datetime.now(datetime.timezone.utc).strftime('%H:%M:%S')} UTC")
        print(f"   Avg SOC:       {fleet_stats['avg_soc']}%")
        print(f"   Charging:      {fleet_stats['vehicles_charging']} vehicles")
        print(f"   En Route:      {fleet_stats['vehicles_en_route']} vehicles")
        print(f"   Idle at Depot: {fleet_stats['vehicles_idle']} vehicles")
        print(f"   Total Sent:    {fleet_stats['total_payloads_sent']} payloads")
        print(f"   Total Errors:  {fleet_stats['total_errors']}")

        low_soc = [b for b in buses if b.soc < 25.0 and not b.charger_connected]
        if low_soc:
            print(f"   ⚠️  LOW SOC: {', '.join(b.vehicle_id for b in low_soc)}")
        print(f"{'─'*70}\n")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
async def main():
    print(f"{'═'*70}")
    print(f"   EV FLEET CHARGING OPTIMIZER — Fleet Manager")
    print(f"   Launching {NUM_VEHICLES} vehicles | Depot: Johannesburg")
    print(f"   Transport: SQS (guaranteed delivery, no dropped payloads)")
    print(f"   Sim multiplier: {SIM_TIME_MULTIPLIER}x")
    print(f"{'═'*70}\n")

    sqs_client = boto3.client('sqs', region_name='eu-west-1')
    buses      = [EVBus(f"Bus-{i:02d}") for i in range(NUM_VEHICLES)]
    tasks      = [run_bus(bus, sqs_client) for bus in buses]
    tasks.append(fleet_monitor(buses))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n🛑 Fleet grounded. "
              f"Total payloads sent: {fleet_stats['total_payloads_sent']}")
        sys.exit(0)