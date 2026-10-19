import logging
import random
import threading
from concurrent.futures import ThreadPoolExecutor

import requests
from psycopg2.extras import RealDictCursor

log = logging.getLogger("outbox_worker")

CLAIM_SQL = """
UPDATE outbox SET locked_until = now() + make_interval(secs => %s), attempts = attempts + 1
WHERE id IN (
    SELECT o.id FROM outbox o
    WHERE o.status = 'pending'
      AND o.next_attempt_at <= now()
      AND (o.locked_until IS NULL OR o.locked_until < now())
      AND (o.depends_on IS NULL
           OR EXISTS (SELECT 1 FROM outbox d WHERE d.id = o.depends_on AND d.status <> 'pending'))
    ORDER BY o.next_attempt_at
    LIMIT %s
    FOR UPDATE SKIP LOCKED
)
RETURNING id, kind, payload, idempotency_key, attempts
"""

MARK_SENT = """
UPDATE outbox SET status = 'sent', sent_at = now(), locked_until = NULL, last_error = NULL
WHERE id = %s AND attempts = %s AND status = 'pending'
"""

MARK_FAILED = """
UPDATE outbox SET locked_until = NULL, last_error = %s,
       next_attempt_at = now() + make_interval(secs => %s),
       status = CASE WHEN attempts >= %s THEN 'dead' ELSE 'pending' END
WHERE id = %s AND attempts = %s AND status = 'pending'
RETURNING status
"""


def http_sender(url, connect_timeout, read_timeout):
    def send(row):
        try:
            response = requests.post(
                url,
                json=row["payload"],
                headers={"Idempotency-Key": row["idempotency_key"]},
                timeout=(connect_timeout, read_timeout),
            )
        except requests.RequestException as exc:
            return False, type(exc).__name__
        if 200 <= response.status_code < 300:
            return True, None
        return False, f"HTTP {response.status_code}"

    return send


class OutboxWorker:
    def __init__(self, db, send, concurrency=8, lease_seconds=30, max_attempts=50,
                 backoff_base=2.0, backoff_cap=300.0, poll_interval=0.5):
        self.db = db
        self.send = send
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.poll_interval = poll_interval
        self._in_flight = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def claim(self, limit):
        with self.db.connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(CLAIM_SQL, (self.lease_seconds, limit))
            return cur.fetchall()

    def backoff(self, attempts):
        return min(self.backoff_cap, self.backoff_base * 2 ** (attempts - 1)) * random.uniform(0.5, 1.0)

    def deliver(self, row):
        try:
            ok, error = self.send(row)
        except Exception as exc:
            ok, error = False, repr(exc)
        try:
            self.finish(row, ok, error)
        except Exception:
            log.exception("could not record result for %s, the lease will expire and it will be retried",
                          row["idempotency_key"])

    def finish(self, row, ok, error):
        key, attempt = row["idempotency_key"], row["attempts"]
        if ok:
            with self.db.connection() as conn, conn.cursor() as cur:
                cur.execute(MARK_SENT, (row["id"], attempt))
            log.info("DELIVERED %s (attempt %d)", key, attempt)
            return

        delay = self.backoff(attempt)
        with self.db.connection() as conn, conn.cursor() as cur:
            cur.execute(MARK_FAILED, (error, delay, self.max_attempts, row["id"], attempt))
            result = cur.fetchone()
        if result and result[0] == "dead":
            log.error("DEAD %s after %d attempts: %s", key, attempt, error)
        else:
            log.warning("FAILED %s (attempt %d): %s, next try in %.1fs", key, attempt, error, delay)

    def run_once(self):
        rows = self.claim(self.concurrency)
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            list(pool.map(self.deliver, rows))
        return len(rows)

    def run(self):
        log.info("outbox worker started (concurrency %d, lease %ds)", self.concurrency, self.lease_seconds)
        with ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix="deliver") as pool:
            while not self._stop.is_set():
                free = self.concurrency - self._in_flight
                if free <= 0:
                    self._stop.wait(0.05)
                    continue
                try:
                    rows = self.claim(free)
                except Exception:
                    log.exception("claim failed, retrying in 2s")
                    self._stop.wait(2)
                    continue
                if not rows:
                    self._stop.wait(self.poll_interval)
                    continue
                for row in rows:
                    with self._lock:
                        self._in_flight += 1
                    pool.submit(self._deliver_and_release, row)
        log.info("outbox worker stopped")

    def _deliver_and_release(self, row):
        try:
            self.deliver(row)
        finally:
            with self._lock:
                self._in_flight -= 1
