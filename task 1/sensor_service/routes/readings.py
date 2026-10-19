from flask import Blueprint, abort, current_app, jsonify, request

from ..ingest import parse_batch
from . import db, ingestor, int_arg, to_camel

bp = Blueprint("readings", __name__)


@bp.post("/readings")
def ingest_readings():
    payload = request.get_json(silent=True)
    if payload is None:
        abort(400, "request body must be JSON")
    items = payload if isinstance(payload, list) else [payload]
    if not items:
        abort(400, "batch is empty")
    max_batch = current_app.config["MAX_BATCH_SIZE"]
    if len(items) > max_batch:
        abort(413, f"a batch may contain at most {max_batch} readings")

    readings, errors = parse_batch(items)
    service = ingestor()
    if errors:
        service.stats.incr("rejected_readings", len(errors))
    if not readings:
        return jsonify(accepted=0, rejected=errors), 400

    outcome = service.submit(readings)
    if outcome == "full":
        response = jsonify(error="ingest queue is full, retry later")
        response.headers["Retry-After"] = "1"
        return response, 429
    if outcome == "stopping":
        return jsonify(error="service is shutting down"), 503
    return jsonify(accepted=len(readings), rejected=errors), 202


@bp.get("/readings")
def list_readings():
    clauses, params = [], []
    for arg, column in (("sensorId", "sensor_id"), ("type", "type")):
        value = request.args.get(arg)
        if value:
            clauses.append(f"{column} = %s")
            params.append(value)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int_arg("limit", default=100, maximum=1000))
    rows = db().fetch_all(
        f"SELECT id, sensor_id, type, value, ts AS timestamp, received_at FROM readings "
        f"{where} ORDER BY ts DESC LIMIT %s",
        params,
    )
    return jsonify(items=[to_camel(row) for row in rows])
