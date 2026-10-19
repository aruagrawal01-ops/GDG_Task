"""Campus sensor simulator.

  python simulator/sensor_sim.py run --sensors 30 --interval 1 --duration 60
  python simulator/sensor_sim.py run --spike library-temperature-1 --spike-value 45 --spike-after 10
  python simulator/sensor_sim.py burst --readings 10000 --batch 500
"""
import argparse
import asyncio
import random
import time
from datetime import datetime, timezone

import requests

BUILDINGS = ("library", "lab", "hostel", "cafeteria", "auditorium")
PROFILES = {
    "temperature": (24.0, 0.4, 15.0, 40.0),
    "air_quality": (45.0, 3.0, 0.0, 300.0),
    "occupancy": (60.0, 5.0, 0.0, 400.0),
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Sensor:
    def __init__(self, sensor_id, sensor_type):
        self.sensor_id = sensor_id
        self.type = sensor_type
        self.mean, self.step, self.low, self.high = PROFILES[sensor_type]
        self.value = self.mean + random.uniform(-2 * self.step, 2 * self.step)

    def read(self, override=None):
        if override is None:
            drift = (self.mean - self.value) * 0.1
            self.value = min(self.high, max(self.low, self.value + drift + random.gauss(0, self.step)))
            value = self.value
        else:
            value = override + random.uniform(-0.5, 0.5)
        return {"sensorId": self.sensor_id, "type": self.type, "value": round(value, 2), "timestamp": now_iso()}


def build_sensors(count):
    types = list(PROFILES)
    sensors = []
    for index in range(count):
        sensor_type = types[index % len(types)]
        building = BUILDINGS[(index // len(types)) % len(BUILDINGS)]
        number = index // (len(types) * len(BUILDINGS)) + 1
        sensors.append(Sensor(f"{building}-{sensor_type}-{number}", sensor_type))
    return sensors


def post_with_backoff(session, url, batch):
    for attempt in range(6):
        response = session.post(url, json=batch, timeout=10)
        if response.status_code != 429:
            return response
        time.sleep(float(response.headers.get("Retry-After", 1)) * (attempt + 1))
    return response


def run(args):
    sensors = build_sensors(args.sensors)
    spiking = set(args.spike or [])
    unknown = spiking - {s.sensor_id for s in sensors}
    if unknown:
        raise SystemExit(f"unknown sensor ids for --spike: {', '.join(sorted(unknown))}")
    print(f"simulating {len(sensors)} sensors, e.g. {', '.join(s.sensor_id for s in sensors[:3])}")

    url = f"{args.url}/readings"
    session = requests.Session()
    started = time.monotonic()
    tick = 0
    while args.duration <= 0 or time.monotonic() - started < args.duration:
        tick += 1
        elapsed = time.monotonic() - started
        spike_on = spiking and elapsed >= args.spike_after
        batch = [s.read(args.spike_value if spike_on and s.sensor_id in spiking else None) for s in sensors]
        response = post_with_backoff(session, url, batch)
        label = " (spiking)" if spike_on else ""
        print(f"tick {tick}: sent {len(batch)} readings -> HTTP {response.status_code}{label}")
        time.sleep(max(0.0, args.interval - (time.monotonic() - started - elapsed)))


def burst(args):
    from loadtest import run_load_test

    asyncio.run(run_load_test(args.url, args.readings, args.batch, args.sensors, args.spread,
                              args.connections, runs=1))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:8000")
    sub = parser.add_subparsers(dest="mode", required=True)

    normal = sub.add_parser("run", help="steady readings, optionally with spiking sensors")
    normal.add_argument("--sensors", type=int, default=30)
    normal.add_argument("--interval", type=float, default=1.0, help="seconds between ticks")
    normal.add_argument("--duration", type=float, default=60, help="seconds, 0 runs forever")
    normal.add_argument("--spike", action="append", metavar="SENSOR_ID", help="sensor to spike (repeatable)")
    normal.add_argument("--spike-value", type=float, default=45.0)
    normal.add_argument("--spike-after", type=float, default=5.0, help="seconds before the spike starts")

    burst_parser = sub.add_parser("burst", help="send a burst of readings within --spread seconds")
    burst_parser.add_argument("--readings", type=int, default=10000)
    burst_parser.add_argument("--batch", type=int, default=500)
    burst_parser.add_argument("--sensors", type=int, default=200)
    burst_parser.add_argument("--spread", type=float, default=1.0)
    burst_parser.add_argument("--connections", type=int, default=100)

    args = parser.parse_args()
    if args.mode == "run":
        run(args)
    else:
        burst(args)


if __name__ == "__main__":
    main()
