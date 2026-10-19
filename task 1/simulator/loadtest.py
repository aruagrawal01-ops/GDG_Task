"""Burst load test: sends N readings within --spread seconds and measures latency, backpressure and drain time.

  python simulator/loadtest.py --readings 10000 --batch 500 --runs 3 --with-rules
"""
import argparse
import asyncio
import json
import platform
import random
import statistics
import time
from collections import Counter
from datetime import datetime, timezone

import aiohttp

HOT_THRESHOLD = 39.0
LOADTEST_RULES = (
    {"name": "loadtest single", "sensorType": "temperature", "kind": "single",
     "operator": ">", "threshold": HOT_THRESHOLD},
    {"name": "loadtest window", "sensorType": "temperature", "kind": "window", "operator": ">",
     "threshold": 39.5, "windowSeconds": 60, "aggregation": "avg", "minSamples": 5},
)


def percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(pct / 100 * len(ordered)) - 1))
    return ordered[index]


def make_batches(prefix, readings, batch_size, sensors):
    timestamp = datetime.now(timezone.utc).isoformat()
    items = [
        {"sensorId": f"{prefix}-s{random.randrange(sensors):03d}", "type": "temperature",
         "value": round(random.uniform(15, 40), 2), "timestamp": timestamp}
        for _ in range(readings)
    ]
    return [items[i:i + batch_size] for i in range(0, readings, batch_size)]


async def get_stats(session, url):
    async with session.get(f"{url}/stats") as response:
        return await response.json()


async def ensure_rules(session, url):
    async with session.get(f"{url}/rules") as response:
        existing = {rule["name"]: rule for rule in (await response.json())["items"]}
    for rule in LOADTEST_RULES:
        current = existing.get(rule["name"])
        if current is None:
            async with session.post(f"{url}/rules", json=rule) as response:
                response.raise_for_status()
        elif not current["enabled"]:
            async with session.post(f"{url}/rules/{current['id']}/enable") as response:
                response.raise_for_status()
    await asyncio.sleep(0.5)


async def send(session, url, batch, start_at, results):
    await asyncio.sleep(max(0.0, start_at - time.perf_counter()))
    started = time.perf_counter()
    try:
        async with session.post(f"{url}/readings", json=batch) as response:
            body = await response.json(content_type=None)
            status = response.status
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        body, status = {}, type(exc).__name__
    results.append((status, time.perf_counter() - started, body.get("accepted", 0) if status == 202 else 0))


async def run_once(session, url, readings, batch_size, sensors, spread, run_label):
    prefix = f"lt{int(time.time())}{run_label}"
    batches = make_batches(prefix, readings, batch_size, sensors)
    hot_sensors = {item["sensorId"] for batch in batches for item in batch if item["value"] > HOT_THRESHOLD}

    async with session.post(f"{url}/stats/reset-peak"):
        pass
    before = await get_stats(session, url)
    results = []
    t0 = time.perf_counter()
    step = spread / len(batches)
    await asyncio.gather(*(send(session, url, batch, t0 + i * step, results) for i, batch in enumerate(batches)))
    send_wall = time.perf_counter() - t0

    accepted = sum(r[2] for r in results)
    target = before["counters"]["writtenReadings"] + accepted
    drain = None
    while time.perf_counter() - t0 < 120:
        current = await get_stats(session, url)
        queue = current["queue"]
        if current["counters"]["writtenReadings"] >= target and queue["depth"] == 0 and queue["inFlight"] == 0:
            drain = time.perf_counter() - t0
            break
        await asyncio.sleep(0.02)
    after = await get_stats(session, url)

    latencies = [r[1] * 1000 for r in results]
    statuses = Counter(r[0] for r in results)
    return {
        "readings": readings,
        "requests": len(batches),
        "batch": batch_size,
        "accepted": accepted,
        "sendWallSeconds": round(send_wall, 3),
        "p50Ms": round(percentile(latencies, 50), 1),
        "p95Ms": round(percentile(latencies, 95), 1),
        "p99Ms": round(percentile(latencies, 99), 1),
        "maxMs": round(max(latencies), 1),
        "meanMs": round(statistics.fmean(latencies), 1),
        "statuses": dict(sorted((str(k), v) for k, v in statuses.items())),
        "throttled429": statuses.get(429, 0),
        "drainSeconds": round(drain, 3) if drain is not None else None,
        "peakQueueDepth": after["queue"]["peakDepth"],
        "alertsCreated": after["counters"]["alertsCreated"] - before["counters"]["alertsCreated"],
        "alertsExpected": len(hot_sensors),
        "maxFlushMs": after["counters"]["maxFlushMs"],
    }


def print_result(index, result):
    print(
        f"run {index}: {result['accepted']}/{result['readings']} readings accepted in "
        f"{result['requests']} requests of {result['batch']} | sent in {result['sendWallSeconds']}s | "
        f"latency p50 {result['p50Ms']}ms p95 {result['p95Ms']}ms p99 {result['p99Ms']}ms max {result['maxMs']}ms | "
        f"statuses {result['statuses']} | drained {result['drainSeconds']}s after start | "
        f"peak queue {result['peakQueueDepth']} | alerts {result['alertsCreated']} "
        f"(expected {result['alertsExpected']})"
    )


async def run_load_test(url, readings, batch, sensors, spread, connections, runs, with_rules=False,
                        warmup=True, output=None):
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=connections)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        if with_rules:
            await ensure_rules(session, url)
        if warmup:
            await run_once(session, url, min(1000, readings), batch, sensors, spread, "w")
        results = []
        for index in range(1, runs + 1):
            result = await run_once(session, url, readings, batch, sensors, spread, f"r{index}")
            print_result(index, result)
            results.append(result)
            await asyncio.sleep(1)

    if output:
        with open(output, "w") as handle:
            json.dump({"host": platform.platform(), "python": platform.python_version(), "runs": results},
                      handle, indent=2)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--readings", type=int, default=10000)
    parser.add_argument("--batch", type=int, default=500)
    parser.add_argument("--sensors", type=int, default=200)
    parser.add_argument("--spread", type=float, default=1.0, help="seconds over which requests are started")
    parser.add_argument("--connections", type=int, default=100)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--with-rules", action="store_true", help="create the load-test alert rules first")
    parser.add_argument("--no-warmup", action="store_true")
    parser.add_argument("--output", help="write results as JSON to this file")
    args = parser.parse_args()
    asyncio.run(run_load_test(args.url, args.readings, args.batch, args.sensors, args.spread,
                              args.connections, args.runs, args.with_rules, not args.no_warmup, args.output))


if __name__ == "__main__":
    main()
