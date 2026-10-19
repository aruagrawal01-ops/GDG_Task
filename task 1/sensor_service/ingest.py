import logging
import math
import threading
import time
from collections import deque
from datetime import datetime, timezone

from psycopg2.extras import RealDictCursor, execute_values

from .engine import Reading, Rule

log = logging.getLogger(__name__)

MAX_ID_LENGTH = 100

INSERT_READINGS = "INSERT INTO readings (sensor_id, type, value, ts) VALUES %s"

UPSERT_ALERTS = """
INSERT INTO alerts (rule_id, sensor_id, message, triggered_value, last_value,
                    trigger_count, reading_ts, last_reading_ts)
VALUES %s
ON CONFLICT (rule_id, sensor_id) WHERE status IN ('open', 'acknowledged') DO UPDATE SET
    message = EXCLUDED.message,
    last_value = EXCLUDED.last_value,
    trigger_count = alerts.trigger_count + EXCLUDED.trigger_count,
    last_reading_ts = GREATEST(alerts.last_reading_ts, EXCLUDED.last_reading_ts),
    last_triggered_at = now()
RETURNING (xmax = 0) AS inserted
"""

HISTORY_SQL = """
SELECT sensor_id, extract(epoch FROM ts)::float8 AS epoch, value
FROM readings
WHERE type = %s AND (%s::text IS NULL OR sensor_id = %s)
  AND ts > now() - make_interval(secs => %s)
ORDER BY ts
"""


def _identifier(item, field):
    value = item.get(field)
    if not isinstance(value, str) or not value.strip() or len(value) > MAX_ID_LENGTH:
        raise ValueError(f"{field} must be a non-empty string of at most {MAX_ID_LENGTH} characters")
    return value


def parse_reading(item):
    if not isinstance(item, dict):
        raise ValueError("reading must be a JSON object")
    sensor_id = _identifier(item, "sensorId")
    reading_type = _identifier(item, "type")

    value = item.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("value must be a finite number")

    raw_ts = item.get("timestamp")
    if not isinstance(raw_ts, str):
        raise ValueError("timestamp must be an ISO 8601 string")
    try:
        ts = datetime.fromisoformat(raw_ts)
    except ValueError:
        raise ValueError("timestamp must be an ISO 8601 string") from None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return Reading(sensor_id, reading_type, float(value), ts)


def parse_batch(items):
    readings, errors = [], []
    for index, item in enumerate(items):
        try:
            readings.append(parse_reading(item))
        except ValueError as exc:
            errors.append({"index": index, "error": str(exc)})
    return readings, errors


class ReadingQueue:
    """Bounded FIFO of batches. Capacity is counted in readings, and a batch is accepted whole or not at all."""

    def __init__(self, capacity):
        self.capacity = capacity
        self._batches = deque()
        self._size = 0
        self._peak = 0
        self._cond = threading.Condition()

    @property
    def depth(self):
        return self._size

    @property
    def peak(self):
        return self._peak

    def offer(self, batch):
        with self._cond:
            if self._size + len(batch) > self.capacity:
                return False
            self._batches.append(batch)
            self._size += len(batch)
            self._peak = max(self._peak, self._size)
            self._cond.notify()
            return True

    def take(self, max_readings, timeout):
        with self._cond:
            if not self._batches:
                self._cond.wait(timeout)
            taken = []
            while self._batches and (not taken or len(taken) + len(self._batches[0]) <= max_readings):
                batch = self._batches.popleft()
                taken.extend(batch)
                self._size -= len(batch)
            return taken

    def reset_peak(self):
        with self._cond:
            self._peak = self._size


class Stats:
    FIELDS = (
        "accepted_readings", "rejected_readings", "throttled_requests", "written_readings",
        "flushes", "failed_flushes", "alerts_created", "alerts_updated",
    )

    def __init__(self):
        self._lock = threading.Lock()
        self._counters = dict.fromkeys(self.FIELDS, 0)
        self.last_flush_ms = 0.0
        self.max_flush_ms = 0.0

    def incr(self, name, amount=1):
        with self._lock:
            self._counters[name] += amount

    def record_flush(self, written, created, updated, elapsed_ms):
        with self._lock:
            self._counters["written_readings"] += written
            self._counters["flushes"] += 1
            self._counters["alerts_created"] += created
            self._counters["alerts_updated"] += updated
            self.last_flush_ms = elapsed_ms
            self.max_flush_ms = max(self.max_flush_ms, elapsed_ms)

    def snapshot(self):
        with self._lock:
            data = dict(self._counters)
            data["last_flush_ms"] = round(self.last_flush_ms, 1)
            data["max_flush_ms"] = round(self.max_flush_ms, 1)
            return data


class Ingestor:
    def __init__(self, db, engine, queue_capacity, flush_max, flush_wait_ms, rule_refresh_seconds):
        self.db = db
        self.engine = engine
        self.queue = ReadingQueue(queue_capacity)
        self.stats = Stats()
        self.flush_max = flush_max
        self.flush_wait = flush_wait_ms / 1000
        self.rule_refresh = rule_refresh_seconds
        self.in_flight = 0
        self._reload = threading.Event()
        self._reload.set()
        self._stopping = threading.Event()
        self._last_rule_load = 0.0
        self._thread = None

    @property
    def alive(self):
        return self._thread is not None and self._thread.is_alive()

    def start(self):
        self._thread = threading.Thread(target=self._run, name="ingest-consumer", daemon=True)
        self._thread.start()

    def stop(self, timeout=30):
        self._stopping.set()
        if self.alive:
            self._thread.join(timeout)
            if self._thread.is_alive():
                log.error("consumer did not drain in %ss, %d readings lost", timeout,
                          self.queue.depth + self.in_flight)

    def submit(self, readings):
        if self._stopping.is_set():
            return "stopping"
        if not self.queue.offer(readings):
            self.stats.incr("throttled_requests")
            return "full"
        self.stats.incr("accepted_readings", len(readings))
        return "accepted"

    def request_reload(self):
        self._reload.set()

    def _run(self):
        pending = []
        backoff = 0.5
        while True:
            if not pending:
                if self._stopping.is_set() and self.queue.depth == 0:
                    break
                pending = self.queue.take(self.flush_max, self.flush_wait)
                self.in_flight = len(pending)
            try:
                self._refresh_rules_if_due()
                if pending:
                    self._flush(pending)
                    pending = []
                    self.in_flight = 0
                backoff = 0.5
            except Exception:
                log.exception("flush failed, retrying %d readings in %.1fs", len(pending), backoff)
                self.stats.incr("failed_flushes")
                self.engine.clear()
                self._reload.set()
                time.sleep(backoff)
                backoff = min(backoff * 2, 5.0)

    def _refresh_rules_if_due(self):
        now = time.monotonic()
        if not self._reload.is_set() and now - self._last_rule_load < self.rule_refresh:
            return
        self._reload.clear()
        with self.db.connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM alert_rules WHERE enabled")
            rules = [Rule.from_row(row) for row in cur.fetchall()]
            self.engine.set_rules(rules, lambda rule: self._load_history(cur, rule))
        self._last_rule_load = now

    @staticmethod
    def _load_history(cur, rule):
        cur.execute(HISTORY_SQL, (rule.sensor_type, rule.sensor_id, rule.sensor_id, rule.window_seconds))
        return [(row["sensor_id"], row["epoch"], row["value"]) for row in cur.fetchall()]

    def _flush(self, readings):
        started = time.perf_counter()
        readings.sort(key=lambda r: r.ts)
        candidates = self.engine.evaluate(readings)
        created = 0
        with self.db.connection() as conn, conn.cursor() as cur:
            execute_values(
                cur, INSERT_READINGS,
                [(r.sensor_id, r.type, r.value, r.ts) for r in readings],
                page_size=1000,
            )
            if candidates:
                rows = execute_values(
                    cur, UPSERT_ALERTS,
                    [(c.rule_id, c.sensor_id, c.message, c.first_value, c.last_value,
                      c.count, c.first_ts, c.last_ts) for c in candidates],
                    page_size=1000,
                    fetch=True,
                )
                created = sum(1 for (inserted,) in rows if inserted)
        elapsed_ms = (time.perf_counter() - started) * 1000
        self.stats.record_flush(len(readings), created, len(candidates) - created, elapsed_ms)
