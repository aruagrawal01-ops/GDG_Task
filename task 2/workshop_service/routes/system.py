from flask import Blueprint, jsonify
from psycopg2 import Error as DatabaseError

from . import db

bp = Blueprint("system", __name__)


@bp.get("/health")
def health():
    try:
        db().fetch_one("SELECT 1")
    except DatabaseError:
        return jsonify(status="degraded", database="unavailable"), 503
    return jsonify(status="ok", database="ok")


@bp.get("/stats")
def stats():
    rows = db().fetch_all(
        "SELECT kind, status, count(*) AS count, max(attempts) AS max_attempts FROM outbox GROUP BY kind, status"
    )
    oldest = db().fetch_one(
        "SELECT extract(epoch FROM now() - min(created_at))::float8 AS seconds FROM outbox WHERE status = 'pending'"
    )
    outbox = {}
    for row in rows:
        outbox.setdefault(row["kind"], {})[row["status"]] = {"count": row["count"],
                                                            "maxAttempts": row["max_attempts"]}
    return jsonify(outbox=outbox, oldestPendingSeconds=oldest["seconds"])
