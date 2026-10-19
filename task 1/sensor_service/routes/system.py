from flask import Blueprint, jsonify
from psycopg2 import Error as DatabaseError

from . import db, ingestor, to_camel

bp = Blueprint("system", __name__)


@bp.get("/health")
def health():
    try:
        db().fetch_one("SELECT 1")
        database = "ok"
    except DatabaseError:
        database = "unavailable"
    consumer = "running" if ingestor().alive else "stopped"
    healthy = database == "ok" and consumer == "running"
    return jsonify(status="ok" if healthy else "degraded", database=database, consumer=consumer), (
        200 if healthy else 503
    )


@bp.get("/stats")
def stats():
    service = ingestor()
    counts = {row["status"]: row["count"] for row in db().fetch_all(
        "SELECT status, count(*) AS count FROM alerts GROUP BY status"
    )}
    return jsonify(
        queue={
            "depth": service.queue.depth,
            "peakDepth": service.queue.peak,
            "capacity": service.queue.capacity,
            "inFlight": service.in_flight,
        },
        counters=to_camel(service.stats.snapshot()),
        engine={"activeRules": service.engine.rule_count, "windows": service.engine.window_count},
        alerts={status: counts.get(status, 0) for status in ("open", "acknowledged", "resolved")},
        consumer="running" if service.alive else "stopped",
    )


@bp.post("/stats/reset-peak")
def reset_peak():
    ingestor().queue.reset_peak()
    return "", 204
