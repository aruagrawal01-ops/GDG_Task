# Task 1: Campus Sensor Monitoring and Alerts

A Flask and PostgreSQL backend that stores sensor readings, checks them against alert rules, and lets organizers manage the alerts.

## Files

| File | What it does |
|---|---|
| `app.py` | The web API: readings, rules and alerts |
| `rules.py` | Checks readings against the rules and creates alerts |
| `db.py` | Opens database connections; `python db.py` creates the tables |
| `schema.sql` | The three tables: `readings`, `rules`, `alerts` |
| `simulator.py` | Fake sensors for testing, plus the 10,000-reading burst test |

## Install and run

You need Python 3.11+ and PostgreSQL with an empty database called `sensors`.

```bash
cd "task 1"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Put your PostgreSQL username and password into `DATABASE_URL` in `.env`, then create the tables once:

```bash
python db.py
```

Start the server with 4 worker processes:

```bash
gunicorn -w 4 -b 127.0.0.1:8000 app:app
```

For quick development you can use `python app.py` instead.

## API

| Request | What it does |
|---|---|
| `POST /readings` | Save one reading or a list of up to 5,000, then check the rules |
| `POST /rules` | Create a rule |
| `GET /rules` | List rules |
| `PUT /rules/<id>` | Edit a rule (send the full rule) |
| `POST /rules/<id>/enable`, `POST /rules/<id>/disable` | Turn a rule on or off |
| `GET /alerts`, `GET /alerts?status=open` | List alerts, newest first |
| `POST /alerts/<id>/acknowledge` | Mark an open alert as acknowledged |
| `POST /alerts/<id>/resolve` | Mark an open or acknowledged alert as resolved |

A reading:

```json
{"sensorId": "library-floor-2", "type": "temperature", "value": 34.5, "timestamp": "2026-10-12T10:30:00Z"}
```

A single-reading rule:

```json
{"name": "Too hot", "type": "temperature", "operator": ">", "threshold": 30}
```

A time-window rule (average over the last 10 minutes):

```json
{"name": "Hot for 10 minutes", "type": "temperature", "operator": ">", "threshold": 28, "window_minutes": 10}
```

Operators are `>`, `>=`, `<` and `<=`.

### Errors

| Status | When |
|---|---|
| 400 | The body is not JSON, or a field is missing or wrong. For readings, `index` says which one. |
| 404 | The rule or alert does not exist |
| 409 | The alert cannot change, for example acknowledging an alert that is already resolved |
| 413 | More than 5,000 readings in one request |
| 503 | The database cannot be reached |

## Simulator

```bash
python simulator.py            # 24 sensors each send a reading every second
python simulator.py --spike    # same, but library-temperature-1 reports 45 degrees
python simulator.py --burst    # 10,000 readings at once, prints a load-test report
```

## How it works

```
sensor -> POST /readings -> check every reading -> one transaction:
                                                     1. bulk INSERT all readings
                                                     2. check enabled rules
                                                     3. INSERT alerts
                                                   -> 201 {"saved": n, "alerts_created": m}
```

- **Batches:** one request can carry up to 5,000 readings. They are saved with one bulk insert (`execute_values`), which is much faster than one insert per reading.
- **All or nothing:** if any reading in a batch is invalid, nothing is saved and the response says which reading is wrong. The readings and the alerts they cause are saved in the same transaction, so a failure never leaves half a batch behind.
- **Single-reading rules** compare each reading with the threshold.
- **Time-window rules** ask PostgreSQL for the sensor's average over the last N minutes, counting back from its newest reading in the batch. One `AVG` query is run per sensor per rule. An index on `(sensor_id, type, timestamp)` keeps this fast.
- **No duplicate alerts:** a partial unique index allows only one unresolved alert per rule and sensor. `ON CONFLICT DO NOTHING` skips the insert when one already exists, so a sensor stuck at 45 degrees creates one alert, not thousands. Once that alert is resolved, a new breach creates a new alert.
- **Alert status changes** use one conditional update (`... WHERE status = ANY(allowed)`), so two organizers clicking at the same time cannot put an alert into a wrong state.
- **No state in memory:** everything is in PostgreSQL, so gunicorn can run several worker processes and a restart loses nothing that was confirmed with 201.

## Load-test report

`python simulator.py --burst` sends 10,000 temperature readings from 200 sensors as 20 requests of 500, all at the same moment.

**Setup:**

- Apple M4 (10 cores), macOS
- Python 3.14
- PostgreSQL 18.1, running locally
- gunicorn with 4 worker processes
- Both rules enabled (one single-reading rule and one 10-minute window rule)

**Results (3 runs):**

| Run | Saved | Failed requests | Total time | Throughput | Median request | Slowest request |
|---|---|---|---|---|---|---|
| 1 | 10,000 | 0 | 0.29 s | 34,275 readings/s | 176 ms | 274 ms |
| 2 | 10,000 | 0 | 0.30 s | 32,971 readings/s | 183 ms | 282 ms |
| 3 | 10,000 | 0 | 0.32 s | 31,615 readings/s | 189 ms | 295 ms |

All 10,000 readings were stored and checked against the rules in about 0.3 seconds, so the requirement is met with room to spare.

## Trade-offs

- **The response waits for the database.** Each request returns only after its readings are saved. Clients get a definite answer and confirmed data is never lost, but each request takes longer than it would with an in-memory queue. At this load that cost is about 0.2 s per request.
- **Batching is needed for the burst.** 10,000 separate one-reading requests per second would be limited by HTTP overhead, not by the database. More gunicorn workers or more servers would help, since the app keeps no state in memory.
- **One window query per sensor per rule.** This is simple and always correct, but with thousands of sensors in one batch it becomes the slowest step. Keeping running averages in memory or in Redis would be the next step.
- **Readings sent twice are stored twice.** A unique constraint on `(sensor_id, timestamp)` would prevent it, at the cost of slower inserts.
