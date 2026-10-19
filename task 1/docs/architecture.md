# Task 1 architecture: Campus Sensor Monitoring and Alerts

## Overview

```
 sensors / simulator
        |  POST /readings  (one object or an array of up to 5,000)
        v
 +--------------------------- one gunicorn process (gthread, 16 threads) ---------------------------+
 |                                                                                                  |
 |  request threads: parse + validate --> bounded queue (50,000 readings) --full--> 429 Retry-After |
 |        |  202 {accepted, rejected}           |                                                   |
 |                                              v                                                   |
 |                         consumer thread: take up to 5,000 readings                               |
 |                              |  sort by timestamp                                                |
 |                              |  rule engine (in memory): single rules + sliding windows          |
 |                              v                                                                   |
 |                 one transaction: bulk INSERT readings + UPSERT alerts (de-duplicated)            |
 +--------------------------------------------------------------------------------------------------+
        |
        v
   PostgreSQL: readings, alert_rules, alerts

 organizers --> /rules (create, edit, enable, disable)  /alerts (list, acknowledge, resolve)
```

The API does as little as possible on the request path. It validates the readings, puts the batch on a bounded in-memory queue, and returns `202`. One consumer thread drains the queue in large chunks. It evaluates the rules and writes the readings and any alerts in a single transaction.

## Components

| File | Responsibility |
|---|---|
| `sensor_service/routes/readings.py` | `POST /readings` and `GET /readings`. Accepts an object or an array, and validates each item. Valid items are accepted and invalid ones are reported by index. |
| `sensor_service/ingest.py` | `ReadingQueue` (bounded, all-or-nothing per batch), `Ingestor` (consumer thread, flush, retry, graceful drain), `Stats` |
| `sensor_service/engine.py` | `RuleEngine`: single-reading rules and time-window rules, with collapsing of duplicate breaches inside a batch |
| `sensor_service/routes/rules.py` | Rule CRUD with validation. Every change triggers a rule reload in the consumer. |
| `sensor_service/routes/alerts.py` | Alert listing with filters and keyset pagination, plus the acknowledge and resolve state machine |
| `sensor_service/routes/system.py` | `/health` and `/stats` (queue depth, peak, counters, flush timings) |
| `simulator/sensor_sim.py` | Realistic sensors (normal, spike, burst) |
| `simulator/loadtest.py` | Burst load test with latency percentiles, drain time and an alert-count check |

## Data model

- `readings(sensor_id, type, value, ts, received_at)`. `ts` is the sensor's own timestamp, stored as `TIMESTAMPTZ`. There are two indexes: `(sensor_id, ts)` for reading lookups and `(type, ts)` for rebuilding windows. More indexes would slow down burst inserts.
- `alert_rules`. `kind` is `single` or `window`. `sensor_id` NULL means every sensor of that type. A `CHECK` makes window rules carry `window_seconds` and `aggregation` (`avg`, `min`, `max`, `sum`, `count`). `min_samples` stops a window with one reading from firing. `updated_at` acts as the rule version.
- `alerts`. `status` is `open`, `acknowledged` or `resolved`. The partial unique index `one_active_alert ON alerts (rule_id, sensor_id) WHERE status IN ('open','acknowledged')` makes the database the single arbiter of de-duplication.

## Rule evaluation

**Single-reading rules** compare each reading with the enabled rules for its type, and its sensor when the rule names one. Rules are cached in memory and grouped by type, so evaluating a reading never queries the database.

**Time-window rules** keep a sliding window per `(rule, sensor)`: a deque of `(timestamp, value)`.

- The window uses the reading's own timestamp, not arrival time, so a delayed batch does not skew it. Each batch is sorted by timestamp first. A late reading that still falls inside the window is inserted in order. A reading older than the window is ignored.
- The window is `(newest - window_seconds, newest]`. Older points are evicted as new ones arrive.
- Each touched `(rule, sensor)` is evaluated once per flush, not after every reading. The aggregate is computed from the deque, so there is no floating-point drift from a running sum. The cost is O(window size) per touched sensor per flush, which is small for realistic sensor rates.
- On startup and whenever a rule changes, the windows for that rule are rebuilt from the `readings` table with one query covering the last `window_seconds`. A restart therefore does not reset a 10-minute average.

**Alerts and de-duplication.** Breaches are first collapsed in memory to one candidate per `(rule, sensor)` per flush. They are then written with:

```sql
INSERT ... ON CONFLICT (rule_id, sensor_id) WHERE status IN ('open','acknowledged')
DO UPDATE SET last_value = ..., trigger_count = alerts.trigger_count + EXCLUDED.trigger_count, ...
```

A stuck sensor therefore produces one alert with a growing `trigger_count`, not thousands of rows. Once an organizer resolves the alert, it leaves the partial index, and the next breach opens a new alert.

**Alert state machine.** The allowed transitions are `open -> acknowledged`, `open -> resolved` and `acknowledged -> resolved`. Each transition is one conditional `UPDATE ... WHERE id = ? AND status IN (...)`. When zero rows change, the API returns `409`, or `404` if the alert does not exist. Two organizers clicking at the same moment cannot corrupt the state.

## Throughput decisions

| Decision | Why |
|---|---|
| Batches on the API | 10,000 readings arrive as 20 requests of 500. Parsing JSON is cheap, while HTTP overhead per request is not. |
| Bounded queue, counted in readings | It absorbs the burst, and memory stays bounded. A batch is accepted whole or rejected whole, so a client never wonders which half got in. |
| `429` with `Retry-After` when full | This is explicit backpressure. Nothing that was accepted is dropped silently. The simulator retries on `429`. |
| Coalesced flushes (up to 5,000 rows) | One `execute_values` insert and one commit per flush, instead of one per reading |
| Rules and windows in memory | No query per reading. Rule changes reach the consumer through a reload flag, and the rules are also refreshed every 5 s. |
| One process with threads (`-w 1 --threads 16`) | There is one queue and one copy of the window state. With several worker processes, each would see only part of a sensor's readings and compute wrong averages. |

## Reliability decisions and trade-offs

- **Accepted is not the same as stored.** `202` means the reading is in memory, not yet in PostgreSQL. On SIGTERM, gunicorn stops taking requests and the consumer drains the queue before the process exits. This was verified with 55,000 queued readings. On `kill -9` or a power cut, whatever is still queued (at most 50,000 readings) is lost. This is the price of absorbing bursts in memory.
- **Database outage.** If a flush fails, the consumer keeps the batch, clears the window state and retries with backoff (0.5 s up to 5 s). The queue fills up, and clients get `429` until PostgreSQL returns, so nothing accepted is dropped while the process is alive. On recovery, windows are rebuilt from the database, so the failed batch is not counted twice.
- **Readings and their alerts commit together.** They are written in one transaction, so an alert never points at a reading that was not stored.
- **Duplicate readings.** A sensor that retries after a timeout can send the same reading twice. Duplicates are stored, because a `UNIQUE (sensor_id, ts)` constraint would slow every insert. Alerts are unaffected because of de-duplication.
- **Window rebuild uses wall-clock time.** After a restart or rule change, a window is reloaded from readings whose timestamps fall in the last `window_seconds` before now. That is right for live sensors. Readings replayed with old timestamps are still stored and evaluated as they arrive, but they are not reloaded into a window after a restart.
- **Window evaluation granularity.** The window is checked at the end of each flush. If an average crosses the threshold and falls back inside a single flush (at most 100 ms of data), that crossing is not reported.

## If readings must never be lost

The alternatives, in order of cost:

1. Insert each batch into PostgreSQL before returning `201`, and run the rules from the consumer afterwards. This removes the crash window. The measured flush time (under 100 ms for 5,000 rows) suggests it would still meet the burst, at the cost of higher request latency.
2. Append batches to a local write-ahead file before acknowledging, and replay it on start.
3. Put a durable log in front (Redis Streams or Kafka). This adds an extra service.

## Scaling further

- **More readings per second.** Use several consumer threads, each owning a set of sensors (hash of `sensorId`). Use `COPY` instead of `execute_values`. Partition `readings` by time for retention.
- **More HTTP requests per second** (10,000 single-reading requests each second). Run several processes and route each sensor to the same process by hashing `sensorId`, or move window state into Redis, or compute windows with SQL per batch. Each option keeps a sensor's readings together.
- **Observability.** `/stats` already exposes queue depth, peak, flush times and alert counters. The next step would be Prometheus metrics and an alert on queue depth.
