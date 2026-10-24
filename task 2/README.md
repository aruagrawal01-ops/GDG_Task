# Task 2: Workshop Registration and Notifications

A Flask and PostgreSQL workshop registration service, a background worker that sends notifications, and a mock notification service that sometimes fails.

## Files

| File | What it does |
|---|---|
| `app.py` | The web API: create workshops, register members, cancel workshops |
| `worker.py` | Sends pending notifications to the notification service and retries failures |
| `notifier.py` | Mock notification service: sometimes returns an error or replies too slowly |
| `db.py` | Opens database connections; `python db.py` creates the tables |
| `schema.sql` | The three tables: `workshops`, `registrations`, `notifications` |

## Install and run

You need Python 3.11+ and PostgreSQL with an empty database called `workshops`.

```bash
cd "task 2"
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Put your PostgreSQL username and password into `DATABASE_URL` in `.env`, then create the tables once:

```bash
python db.py
```

Start each program in its own terminal:

```bash
python notifier.py
```

```bash
python app.py
```

```bash
python worker.py
```

| Program | Address |
|---|---|
| API | http://localhost:8001 |
| Mock notifier | http://localhost:8002 |

The notifier fails 20% of requests and replies too slowly to 10%. Change this with environment variables, for example `FAILURE_RATE=0.5 SLOW_RATE=0.2 python notifier.py`.

## API

| Request | What it does |
|---|---|
| `POST /workshops` | Create a workshop: `{"title": "Intro to Backend"}` |
| `GET /workshops/<id>` | The workshop and its registrations |
| `POST /workshops/<id>/register` | Register a member: `{"member_id": "alice"}` |
| `POST /workshops/<id>/cancel` | Cancel the workshop and notify every registered member |
| `GET /notifications` | Every notification with its status, attempts and last error |

The mock notifier has `POST /notify` (used by the worker) and `GET /deliveries` (what it has delivered).

### Errors

| Status | When |
|---|---|
| 400 | The body is missing or a field is empty |
| 404 | The workshop does not exist |
| 409 | The member is already registered, the workshop is cancelled, or it is already cancelled |
| 503 | The database cannot be reached |

## Demonstration

With the three programs running:

**1. Registration.** Each request returns 201 in about 20 ms, without waiting for the notifier:

```bash
curl -X POST localhost:8001/workshops -H 'content-type: application/json' -d '{"title": "Intro to Backend"}'
curl -X POST localhost:8001/workshops/1/register -H 'content-type: application/json' -d '{"member_id": "alice"}'
curl -X POST localhost:8001/workshops/1/register -H 'content-type: application/json' -d '{"member_id": "bob"}'
curl localhost:8001/notifications
```

Registering `alice` again returns 409, and no second notification is created.

**2. Notification failure.** Stop `notifier.py` with Ctrl+C and register another member. In `GET /notifications` the new confirmation stays `pending`, `attempts` goes up and `last_error` shows `ConnectionError`. The worker log shows:

```
FAILED  confirmation for dave (attempt 1): ConnectionError, retrying in 2s
FAILED  confirmation for dave (attempt 2): ConnectionError, retrying in 4s
```

**3. Recovery.** Start `notifier.py` again. At its next retry the confirmation is sent:

```
SENT    confirmation for dave (attempt 4)
```

**4. Backend restart.** Stop the notifier, register a member, then kill `app.py` and `worker.py` (Ctrl+C, or `kill -9`). Start all three again. The pending confirmation is still in the database and gets delivered.

**5. Cancellation.** Cancel the workshop while the notifier is flaky:

```bash
curl -X POST localhost:8001/workshops/1/cancel
```

The response says how many notifications were queued. Every registered member gets a cancellation notification. Members whose sends fail are retried while the others go through, as in this run with a 40% failure rate:

```
SENT    cancellation for alice (attempt 1)
FAILED  cancellation for bob (attempt 1): ReadTimeout, retrying in 2s
SENT    cancellation for carol (attempt 1)
FAILED  cancellation for dave (attempt 1): HTTP 500, retrying in 2s
SENT    cancellation for erin (attempt 1)
SENT    cancellation for bob (attempt 2)
```

## How the design handles the hard cases

### The registration and its notification are saved together

Registering inserts the registration and a `pending` row in `notifications` in **one transaction**. Either both are saved or neither is:

- A **failed registration** (duplicate, cancelled workshop, unknown workshop) is rolled back, so it never produces a confirmation.
- A **successful registration** always has a notification row waiting. The API never calls the notifier itself, so it answers immediately.

### Lost notifications

A notification is never only in memory. It stays `pending` in the database until the notifier answers with a 2xx status. Nothing is lost if the notifier is down, slow, or returns an error.

### Retries

The worker picks the next due notification, sends it with a 2-second timeout, and records the result:

- A **2xx response** marks it `sent`.
- An **error, a timeout or no connection** adds one to `attempts`, stores the error, and schedules the next try with a growing wait of 2, 4, 8, 16, 32, then 60 seconds.

Each notification has its own `attempts` and `next_attempt_at`. While one member's notification waits to be retried, the worker carries on with everyone else's, so **one failure never blocks the others**.

### Duplicate delivery

When a request times out, the worker cannot know whether the notifier received it. It retries, because "every registration must eventually get a confirmation". So a message may arrive more than once (at-least-once delivery).

To make this harmless, each notification is sent with an `Idempotency-Key` header (`notification-<id>`) that stays the same on every retry. The notifier remembers the keys it has delivered and ignores repeats:

```
DELIVERED  notification-7: cancellation for bob
SLOW       notification-7: replying after 5s, the worker will time out
DUPLICATE  notification-7: already delivered, ignored
```

The database also allows only one confirmation and one cancellation per registration (`UNIQUE (registration_id, kind)`).

### Backend restarts

| The backend stops… | What happens |
|---|---|
| Before the registration is committed | Nothing was saved and the client got no 201, so no confirmation is owed |
| After the commit, before sending | The notification is `pending` in the database; the worker sends it after the restart |
| While the worker is sending | The worker's transaction is rolled back, so the notification is still `pending` and is sent again; a repeat is ignored by the notifier |

### Registering and cancelling at the same time

Registration locks the workshop row with `FOR SHARE` and cancellation with `FOR UPDATE`. A registration therefore either finishes before the cancellation (and that member gets a cancellation notice) or sees the workshop as cancelled and gets 409.

### Several workers

The worker picks notifications with `FOR UPDATE SKIP LOCKED`, so more than one worker can run without sending the same notification at the same time.

## Trade-offs

- **The worker sends one notification at a time.** A slow notifier (2-second timeout) limits throughput. Running more workers fixes this.
- **Retries never stop.** This keeps the "eventually delivered" promise, but a permanently broken address would be retried every minute forever. A real system would stop after N attempts and alert someone.
- **Order is not guaranteed.** If a confirmation is still being retried when the workshop is cancelled, the member may get the cancellation first.
- **The mock notifier remembers delivered keys only while it is running.** A real notification service would store them in a database.
