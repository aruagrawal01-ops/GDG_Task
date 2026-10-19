# Task 1: Campus Sensor Monitoring and Alerts

A Flask and PostgreSQL backend. It ingests campus sensor readings (single readings or batches), evaluates organizer-defined alert rules (single-reading and time-window), and lets organizers view, acknowledge and resolve alerts. It includes a sensor simulator and a burst load test.

- Architecture and trade-offs: [docs/architecture.md](docs/architecture.md)
- Load-test report (10,000 readings in one second): [docs/load-test-report.md](docs/load-test-report.md)
- Postman collection: [postman/sensor-service.postman_collection.json](postman/sensor-service.postman_collection.json)

## Prerequisites

- Python 3.11 or newer (tested on 3.14)
- PostgreSQL 13 or newer (tested on 14). The repository's `docker-compose.yml` starts one with both databases.

## Install and run

```bash
cd "task 1"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env          # set DATABASE_URL to your PostgreSQL database
gunicorn -c gunicorn.conf.py wsgi:app
```

The service listens on `http://localhost:8000` and creates its tables on startup. Check it with:

```bash
curl localhost:8000/health
curl localhost:8000/stats
```

For development you can also run `flask --app wsgi run --port 8000`. Do not use `--debug`, because the reloader would start the consumer twice.

### Configuration (`.env`)

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres@localhost:5432/sensors` | PostgreSQL connection string |
| `QUEUE_CAPACITY` | `50000` | Maximum readings waiting in memory before the API returns `429` |
| `FLUSH_MAX_READINGS` | `5000` | Maximum readings per database transaction |
| `FLUSH_WAIT_MS` | `100` | How long the consumer waits for work when the queue is empty |
| `MAX_BATCH_SIZE` | `5000` | Maximum readings in one request (`413` above this) |
| `RULE_REFRESH_SECONDS` | `5` | Periodic rule reload. Changes made through the API apply immediately. |
| `DB_POOL_MAX` | `24` | Connection pool size. Keep it above `GUNICORN_THREADS`. |
| `GUNICORN_THREADS` | `16` | Request threads in the single worker process |

## API

| Method and path | Purpose |
|---|---|
| `POST /readings` | Body is one reading or an array. Returns `202 {"accepted": n, "rejected": [{"index", "error"}]}`, `400` if nothing is valid, `413` if the batch is too large, or `429` with `Retry-After` when the queue is full. |
| `GET /readings?sensorId=&type=&limit=` | Latest stored readings |
| `POST /rules`, `GET /rules?enabled=&sensorType=` | Create and list rules |
| `GET /rules/{id}`, `PUT` or `PATCH /rules/{id}` | View and edit a rule. Edits are partial: send only the fields to change. |
| `POST /rules/{id}/enable`, `POST /rules/{id}/disable` | Toggle a rule |
| `GET /alerts?status=&sensorId=&ruleId=&limit=&beforeId=` | List alerts, newest first, with keyset pagination through `nextBeforeId` |
| `GET /alerts/{id}` | One alert |
| `POST /alerts/{id}/acknowledge`, `POST /alerts/{id}/resolve` | State changes. `409` if not allowed from the current state. |
| `GET /health`, `GET /stats` | Liveness, queue depth, counters and flush timings |

Reading:

```json
{"sensorId": "library-floor-2", "type": "temperature", "value": 34.5, "timestamp": "2026-10-12T10:30:00Z"}
```

Single-reading rule:

```json
{"name": "Too hot", "sensorType": "temperature", "kind": "single", "operator": ">", "threshold": 30}
```

Time-window rule. `sensorId` is optional; leave it out to apply the rule to every sensor of the type.

```json
{"name": "Hot for 10 minutes", "sensorType": "temperature", "kind": "window", "operator": ">",
 "threshold": 28, "windowSeconds": 600, "aggregation": "avg", "minSamples": 3}
```

Operators are `>`, `>=`, `<` and `<=`. Aggregations are `avg`, `min`, `max`, `sum` and `count`.

## Simulator

```bash
# 30 sensors (temperature, air quality, occupancy) sending a reading every second for a minute
python simulator/sensor_sim.py run --sensors 30 --interval 1 --duration 60

# same, but lab-temperature-1 jumps to about 45 after 10 seconds
python simulator/sensor_sim.py run --spike lab-temperature-1 --spike-value 45 --spike-after 10

# a burst of 10,000 readings within one second
python simulator/sensor_sim.py burst --readings 10000 --batch 500
```

The simulator retries with backoff when it receives `429`.

## Load test

```bash
python simulator/loadtest.py --runs 3 --with-rules
```

The script prints accepted readings, latency p50, p95 and p99, status codes, drain time, peak queue depth, and alerts created against the expected count. The results are in [docs/load-test-report.md](docs/load-test-report.md).

## Demo walkthrough

1. Start the service. Show `/health` and `/stats`.
2. Create the single rule (`temperature > 30`) and the window rule (`avg over 600 s > 28`) shown above.
3. Post the example reading (34.5). `GET /alerts?status=open` shows one alert for each rule.
4. Post the same breaching reading again. There is still one alert per rule, and `triggerCount` went up.
5. Run the simulator with `--spike`. The window rule opens one alert for the spiking sensor only.
6. Acknowledge the alert, then resolve it. Acknowledging it again returns `409`.
7. Disable the rule and post a breaching reading: no new alert. Enable it again.
8. Run the burst: `python simulator/loadtest.py --runs 1 --with-rules`.

## Tests

The tests need a separate, empty PostgreSQL database:

```bash
createdb sensors_test
TEST_DATABASE_URL=postgresql://postgres@localhost:5432/sensors_test pytest
```

They cover the rule engine (single rules, windows, eviction, out-of-order and late readings, min samples, rebuild after a rule change) and the API: validation, `429` backpressure, rule CRUD, alert de-duplication, the alert lifecycle, disabled rules, window alerts, window state after a restart, the queue drain on shutdown, retry after a database failure, filters and pagination.
