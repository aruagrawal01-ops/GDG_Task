# GDG Backend Development Tasks

Two backend services built with Python, Flask and PostgreSQL.

| Folder | Task | Highlights |
|---|---|---|
| [`task 1`](task%201/) | Campus Sensor Monitoring and Alerts | Batched ingestion through a bounded queue with `429` backpressure; single-reading and sliding time-window rules; de-duplicated alerts with an open, acknowledged and resolved lifecycle; a sensor simulator; a measured load test of 10,000 readings in one second |
| [`task 2`](task%202/) | Workshop Registration and Notifications | Transactional outbox; a separate outbox worker with `SKIP LOCKED` claims, leases, backoff with jitter and dead letters; an idempotent mock notifier that fails and times out; an automated demo including a `kill -9` restart |

Each task folder has its own README with install and run instructions, an architecture document, a Postman collection, and a test suite.

- Task 1: [README](task%201/README.md), [architecture](task%201/docs/architecture.md), [load-test report](task%201/docs/load-test-report.md)
- Task 2: [README](task%202/README.md), [architecture: lost notifications, retries, duplicates and restarts](task%202/docs/architecture.md)

## Quick start

You need Python 3.11 or newer, and PostgreSQL. To get PostgreSQL from Docker, with the `sensors` and `workshops` databases and their test databases created:

```bash
docker compose up -d
```

Then, for each task:

```bash
cd "task 1"            # or "task 2"
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # points at the docker-compose database by default
```

Then follow that task's README.

## Ports

| Service | Port |
|---|---|
| Task 1 sensor API | 8000 |
| Task 2 registration API | 8001 |
| Task 2 mock notifier | 8002 |
| PostgreSQL | 5432 |
