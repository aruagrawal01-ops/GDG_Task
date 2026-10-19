# Task 2 architecture: Workshop Registration and Notifications

## Overview

```
client --POST /workshops/{id}/register--> API (gunicorn)
                                           |  BEGIN
                                           |    INSERT registrations          (UNIQUE workshop_id, member_id)
                                           |    UPDATE workshops seats/status  (row lock, capacity check)
                                           |    INSERT outbox 'confirmation'   (idempotency key confirmation:<registration id>)
                                           |  COMMIT
                                           v
                                     201 returned at once; the notifier is never called on the request path

outbox worker (separate process)
   loop: claim due rows   UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED) -- sets a 30 s lease
         send each row    POST /notify with header Idempotency-Key, timeout 1 s connect / 2 s read, 8 in parallel
         2xx           -> status = 'sent'
         error/timeout -> attempts + 1, next_attempt_at = now + backoff with jitter, 'dead' after MAX_ATTEMPTS
                                           |
                                           v
                     mock notifier: random 500s, slow replies, can be switched off,
                     remembers every idempotency key in SQLite
```

There are three processes: the API (`wsgi.py`), the outbox worker (`python -m outbox_worker`) and the mock notifier (`python -m mock_notifier`). PostgreSQL is the only shared state.

## Data model

| Table | Constraint doing the real work |
|---|---|
| `workshops` | `status IN ('active','cancelled')`, optional `capacity` with `seats_taken <= capacity` |
| `members` | `email UNIQUE` |
| `registrations` | `UNIQUE (workshop_id, member_id)`: a member can register only once, even under concurrent requests |
| `outbox` | `idempotency_key UNIQUE` (`confirmation:<registration id>`, `cancellation:<registration id>`), a per-row retry state (`status`, `attempts`, `next_attempt_at`, `locked_until`, `last_error`), and `depends_on` to order a member's cancellation after their confirmation |

## The transactional outbox

A registration has two side effects: a database row and an HTTP call. These cannot be made atomic, so every naive ordering has a failure window:

- Calling the notifier first, then saving: if the save fails, the member was told they are registered when they are not.
- Saving first, then calling the notifier: if the process crashes between the two, the confirmation is never sent.
- Saving first, then handing the call to an in-memory queue: if the process restarts, the queue is empty and the confirmation is lost.

Instead, the registration and an outbox row saying "a confirmation is owed" are committed in the same transaction. Either both exist or neither does.

- A **failed registration** (duplicate, full, cancelled, unknown member) rolls back, so it leaves no outbox row and produces no confirmation.
- A **successful registration** always has a durable outbox row, which the worker keeps delivering until the notifier acknowledges it.
- The API **never waits for the notifier**. In the demo, registration takes 3 to 9 ms whether the notifier is up, slow or down.

## Concurrency control

- **Duplicate registration.** The `UNIQUE (workshop_id, member_id)` constraint is the arbiter, not a check in Python. In the tests, 20 concurrent attempts by the same member produce exactly one registration and one outbox row.
- **Capacity.** `UPDATE workshops SET seats_taken = seats_taken + 1 WHERE id = ? AND status = 'active' AND (capacity IS NULL OR seats_taken < capacity)` is a conditional update, and PostgreSQL serialises it on the row lock. With 20 concurrent registrations for 5 seats, exactly 5 succeed.
- **Cancellation racing registrations.** Both operations update the same workshop row, so they serialise on its row lock. A registration that commits first is seen by the cancel's `INSERT ... SELECT` and gets a cancellation notice. A registration that waits behind the cancel re-reads `status = 'cancelled'`, fails with `409` and rolls back. In the tests, 30 registrations race a cancel, and every registration that survives has exactly one confirmation and one cancellation.

## Delivery: claims, leases and retries

- **Claim, then send outside the transaction.** The worker claims due rows in a short transaction, setting `locked_until = now() + 30 s` and `attempts + 1`, and commits. It makes the HTTP call afterwards, so no database lock is held across a network call.
- **`FOR UPDATE SKIP LOCKED`** lets several workers run at once without waiting on each other or taking the same row.
- **Leases heal crashes.** If a worker dies after claiming a row, `locked_until` passes and any worker can claim the row again.
- **Fencing.** A result is recorded only `WHERE attempts = <the attempt this worker claimed>`. A worker that comes back after its lease expired cannot overwrite the newer attempt's state.
- **Per-row retry state.** Each member's notification has its own `attempts` and `next_attempt_at`. The worker sends up to 8 rows in parallel and claims new work as soon as a slot frees up, so a member whose delivery keeps failing or timing out never blocks the others. The test `test_one_failing_member_does_not_block_others` checks this.
- **Backoff with jitter.** The delay is `min(cap, base * 2^(attempt-1)) * random(0.5, 1.0)`, with base 2 s and cap 300 s by default (the demo uses 0.5 s and 3 s). Jitter stops all failed rows from retrying at the same instant when the notifier recovers.
- **Timeouts are mandatory.** Each call uses a 1 s connect and 2 s read timeout. Only a 2xx response counts as success. A timeout, a connection error and any other status code all count as failures to retry.
- **"Eventually" versus dead letters.** After `MAX_ATTEMPTS` (default 50, which is several hours at the 300 s cap), a row becomes `dead`. It stays visible in `GET /outbox?status=dead` and in `/stats`, and `POST /outbox/{id}/retry` replays it. This is a deliberate trade-off: "eventually" with a finite budget plus a manual replay, instead of hammering a notifier that is broken for good.

## Lost notifications, duplicates and restarts

**Lost notifications.** A notification is never held only in memory. It exists as an outbox row from the moment the registration commits. It leaves the `pending` state only when the notifier returns 2xx, or when retries are exhausted, in which case it becomes `dead`, is visible, and can be replayed.

**Duplicate delivery.** Exactly-once delivery is impossible over an unreliable network. When a call times out, the worker cannot tell whether the request was lost or the reply was lost. So delivery is **at-least-once**, and duplicates are made harmless:

- The idempotency key is derived from business identity (`confirmation:<registration id>`), not generated per attempt, so every retry carries the same key.
- The notifier stores every key it has delivered, in SQLite so this survives a notifier restart. A repeated key gets `200 {"status":"duplicate"}` and is not delivered again. Demo step 4 forces exactly this case: the notifier delivers, replies after the worker's timeout, the worker retries, and the notifier logs `DUPLICATE-IGNORED`.
- On the sender side, `UNIQUE (idempotency_key)` means a logical message is queued only once. For example, cancelling twice queues nothing the second time.

The effect for the member is exactly one confirmation and one cancellation.

**Backend restarts.** Every point where a crash can land:

| Crash point | State left behind | What happens next |
|---|---|---|
| Before the registration commits | Nothing | The client got no success. No registration and no confirmation, which is correct. |
| After commit, before any send | Outbox row `pending` | The worker picks it up after restart. Demo step 7 shows this with `kill -9`. |
| Worker claimed the row, then died | Row leased, `attempts` incremented | The lease expires and another claim picks it up. |
| Notifier delivered, worker died before marking `sent` | Row still `pending` | It is sent again, and the notifier ignores it as a duplicate. |
| Notifier delivered, but the reply timed out | Worker records a failure | Retry, and the duplicate is absorbed by the key. |
| Notifier down for minutes | Rows `pending`, `attempts` growing, backoff | Rows drain when it returns (demo steps 5 and 6). |
| Database down | API returns `503`, worker cannot claim and logs | Nothing is lost. Work resumes when the database returns. |
| Cancel crashes mid-transaction | Rolled back entirely | Call cancel again. It is idempotent. |

On SIGTERM the worker stops claiming and finishes in-flight sends before exiting. On `kill -9`, in-flight rows are recovered through the lease.

## Cancellation

One transaction does two things. It sets the workshop to `cancelled`, and it inserts one `cancellation` outbox row per registration with `INSERT ... SELECT ... ON CONFLICT (idempotency_key) DO NOTHING`. Either the workshop is cancelled and every member has a pending notification, or nothing changed.

**Pending confirmations are still delivered.** The requirement says every successful registration must eventually produce a confirmation, so a confirmation that is still pending at cancellation time is not dropped. To keep the messages in a sensible order, each cancellation row has `depends_on` pointing at that member's confirmation. The worker claims it only after the confirmation is `sent` (or `dead`). Every member therefore receives "confirmed" before "cancelled", and a member whose confirmation keeps failing does not hold up anyone else's cancellation notice. An alternative is to mark pending confirmations as superseded. That sends fewer messages but breaks the "every registration gets a confirmation" rule.

## Observability

- `GET /outbox?status=&kind=&workshopId=` shows each row's status, attempts, next attempt and last error.
- `GET /stats` shows counts per kind and status, the highest attempt count, and the age of the oldest pending row (the key alerting signal).
- The worker logs `DELIVERED`, `FAILED ... next try in`, and `DEAD` with the idempotency key and attempt number. The notifier logs `DELIVERED`, `DUPLICATE-IGNORED`, `FAILED (injected 500)` and `SLOW REPLY`. Reading the two logs side by side makes retries and de-duplication visible.

## What I would do next

- A circuit breaker in the worker, so it stops calling a notifier that is clearly down and probes occasionally.
- An alert on `oldestPendingSeconds` and on dead rows.
- Clean-up or partitioning of old `sent` rows.
- Authentication for organizer endpoints (create and cancel workshops) and for members (register themselves only).
