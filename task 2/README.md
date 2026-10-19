# Task 2: Workshop Registration and Notifications

A workshop registration service (Flask and PostgreSQL), an outbox worker that delivers notifications with retries, and a mock HTTP notification service that sometimes fails or times out. Registration returns immediately. Confirmations and cancellation notices are delivered eventually, survive backend restarts, and are de-duplicated by idempotency keys.

- Design, and how it handles lost notifications, retries, duplicate delivery and restarts: [docs/architecture.md](docs/architecture.md)
- Postman collection: [postman/workshop-service.postman_collection.json](postman/workshop-service.postman_collection.json)

## Components

| Process | Command | Port |
|---|---|---|
| Registration API | `gunicorn -c gunicorn.conf.py wsgi:app` | 8001 |
| Outbox worker | `python -m outbox_worker` | none |
| Mock notifier | `python -m mock_notifier` | 8002 |

## Prerequisites

- Python 3.11 or newer (tested on 3.14)
- PostgreSQL 13 or newer (tested on 14). The repository's `docker-compose.yml` starts one.

## Install and run

```bash
cd "task 2"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # set DATABASE_URL to your PostgreSQL database
```

Start each process in its own terminal from the `task 2` folder:

```bash
python -m mock_notifier
```

```bash
gunicorn -c gunicorn.conf.py wsgi:app
```

```bash
python -m outbox_worker
```

The API and the worker create the tables on startup. Check them with `curl localhost:8001/health` and `curl localhost:8001/stats`.

### Configuration (`.env`)

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgresql://postgres@localhost:5432/workshops` | PostgreSQL connection string |
| `NOTIFIER_URL` | `http://localhost:8002/notify` | Where the worker sends notifications |
| `WORKER_CONCURRENCY` | `8` | Notifications sent in parallel |
| `LEASE_SECONDS` | `30` | How long a claimed row stays locked if the worker dies |
| `MAX_ATTEMPTS` | `50` | Attempts before a row becomes `dead` (it can be replayed) |
| `BACKOFF_BASE_SECONDS`, `BACKOFF_CAP_SECONDS` | `2`, `300` | Exponential backoff with jitter |
| `NOTIFY_CONNECT_TIMEOUT`, `NOTIFY_READ_TIMEOUT` | `1`, `2` | Per-call timeouts in seconds |
| `NOTIFIER_FAILURE_RATE` | `0.2` | Share of calls that return `500` |
| `NOTIFIER_TIMEOUT_RATE` | `0.15` | Share of calls that are delivered but answered only after `NOTIFIER_TIMEOUT_SECONDS` (5) |
| `NOTIFIER_DB` | `notifier.sqlite3` | Where the notifier remembers delivered idempotency keys |

## Demonstration

The demo script starts all three processes itself (stop any running copies first). It walks through every required scenario and prints the outbox and the notifier's view at each step:

```bash
python simulator/demo.py
```

1. Start the notifier, the API and the worker, and show `/health`.
2. Register three members. Each `201` comes back in a few milliseconds, and the confirmations move to `sent`.
3. Register a member twice. The second attempt gets `409`, and no new outbox row appears.
4. Lost acknowledgement: the notifier delivers but replies after the worker's timeout. The worker retries, the notifier logs `DUPLICATE-IGNORED`, and the member is notified once.
5. Notification failure: the notifier is stopped. New confirmations stay `pending` while `attempts` grows with backoff.
6. Recovery: the notifier is started again, and the pending rows drain.
7. Backend restart: a member registers, then the API and the worker are killed with `kill -9`. The outbox row is still in PostgreSQL. After restart, the confirmation is delivered.
8. Cancellation with 50% notifier errors: one cancellation row per member. All are eventually `sent`. A second cancel queues nothing.
9. The notifier's record: every member got exactly one confirmation and one cancellation, in that order.

Process logs go to `demo-logs/`. Reading `worker.log` and `notifier.log` side by side shows the retries and de-duplication.

### Doing it by hand (curl or Postman)

```bash
curl -X POST localhost:8001/members -H 'content-type: application/json' -d '{"name":"Asha","email":"asha@campus.edu"}'
curl -X POST localhost:8001/workshops -H 'content-type: application/json' -d '{"title":"Backend Basics","startsAt":"2026-11-20T10:00:00Z","capacity":30}'
curl -X POST localhost:8001/workshops/1/register -H 'content-type: application/json' -d '{"memberId":1}'
curl "localhost:8001/outbox?workshopId=1"
curl -X POST localhost:8002/admin/config -H 'content-type: application/json' -d '{"down":true}'   # notifier unavailable
curl -X POST localhost:8002/admin/config -H 'content-type: application/json' -d '{"down":false,"failureRate":0.5}'
curl -X POST localhost:8001/workshops/1/cancel
curl "localhost:8002/deliveries?workshopId=1"
```

## API

| Method and path | Purpose |
|---|---|
| `POST /members`, `GET /members`, `GET /members/{id}` | Create and view members. Email is unique (`409`). |
| `POST /workshops`, `GET /workshops`, `GET /workshops/{id}` | Create and view workshops (`title`, optional `startsAt` and `capacity`) |
| `POST /workshops/{id}/register` | Body `{"memberId": 1}`. Returns `201`, `404` (unknown workshop or member), or `409` (already registered, full, or cancelled). |
| `GET /workshops/{id}/registrations` | Registrations with each member's confirmation and cancellation delivery status |
| `POST /workshops/{id}/cancel` | Cancels and queues one notification per registered member. Idempotent. |
| `GET /outbox?status=&kind=&workshopId=` | Debug view of notifications, including attempts and last error |
| `POST /outbox/{id}/retry` | Replays a `dead` notification |
| `GET /health`, `GET /stats` | Liveness, outbox counts per kind and status, and the age of the oldest pending row |

Mock notifier (port 8002):

| Method and path | Purpose |
|---|---|
| `POST /notify` | Requires an `Idempotency-Key` header. Returns `200 delivered`, `200 duplicate`, `500` (injected), or `503` (down). It can also deliver and then reply slowly. |
| `GET /deliveries?workshopId=&memberId=` | What was actually delivered, with a duplicate count per key |
| `GET /stats` | Settings and counters |
| `POST /admin/config` | Change `failureRate`, `timeoutRate`, `timeoutSeconds`, `down`, `failNext` or `timeoutNext` at runtime |
| `POST /admin/reset` | Clear the delivery record |

## Tests

The tests need a separate, empty PostgreSQL database:

```bash
createdb workshops_test
TEST_DATABASE_URL=postgresql://postgres@localhost:5432/workshops_test pytest
```

They cover:

- The outbox row being written atomically with the registration, and no row for failed registrations
- Concurrent duplicate registrations, the capacity limit under concurrency, and cancellation racing registrations
- Cancellation ordering and idempotency
- Delivery, backoff, per-member isolation of failures, dead letters and replay, sender exceptions
- Lease expiry with a stale result being ignored, and restarts
- Notifier idempotency, including across a notifier restart
- A real HTTP lost-acknowledgement scenario
