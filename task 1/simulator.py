"""Sensor simulator.

python simulator.py           24 sensors each send one reading every second
python simulator.py --spike   same, but library-temperature-1 reports 45 degrees
python simulator.py --burst   sends 10,000 readings at once and prints a load-test report
"""
import random
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

URL = "http://localhost:8000/readings"
SENSOR_TYPES = {"temperature": (22, 26), "air_quality": (30, 60), "occupancy": (0, 120)}
PLACES = ["library", "lab", "hostel", "canteen"]


def make_reading(sensor_id, reading_type, value=None):
    low, high = SENSOR_TYPES[reading_type]
    return {
        "sensorId": sensor_id,
        "type": reading_type,
        "value": round(value if value is not None else random.uniform(low, high), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(spike):
    sensors = [(f"{place}-{kind}-{n}", kind) for place in PLACES for kind in SENSOR_TYPES for n in (1, 2)]
    print(f"Simulating {len(sensors)} sensors. Press Ctrl+C to stop.")
    while True:
        batch = []
        for sensor_id, kind in sensors:
            value = 45 if spike and sensor_id == "library-temperature-1" else None
            batch.append(make_reading(sensor_id, kind, value))
        try:
            response = requests.post(URL, json=batch, timeout=5)
            print(f"sent {len(batch)} readings -> {response.status_code} {response.json()}")
        except requests.RequestException as exc:
            print(f"server not reachable: {exc}")
        time.sleep(1)


def send_batch(batch):
    started = time.perf_counter()
    try:
        status = requests.post(URL, json=batch, timeout=30).status_code
    except requests.RequestException:
        status = "error"
    return status, (time.perf_counter() - started) * 1000


def burst(total=10000, batch_size=500, sensors=200):
    readings = [make_reading(f"burst-temp-{random.randrange(sensors)}", "temperature") for _ in range(total)]
    batches = [readings[i:i + batch_size] for i in range(0, total, batch_size)]

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(batches)) as pool:
        results = list(pool.map(send_batch, batches))
    total_seconds = time.perf_counter() - started

    latencies = sorted(ms for _, ms in results)
    statuses = [status for status, _ in results]
    saved = statuses.count(201) * batch_size
    print(f"Sent {total} readings as {len(batches)} requests of {batch_size}")
    print(f"Saved: {saved}   Failed requests: {len(statuses) - statuses.count(201)}")
    print(f"Total time: {total_seconds:.2f} s   Throughput: {saved / total_seconds:.0f} readings/s")
    print(f"Request latency: median {statistics.median(latencies):.0f} ms, slowest {latencies[-1]:.0f} ms")


if __name__ == "__main__":
    if "--burst" in sys.argv:
        burst()
    else:
        run(spike="--spike" in sys.argv)
