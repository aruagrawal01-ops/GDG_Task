import math

from flask import Blueprint, abort, jsonify, request

from ..engine import AGGREGATIONS, OPERATORS
from . import db, ingestor, json_body, to_camel

bp = Blueprint("rules", __name__)

FIELDS = {
    "name": "name",
    "sensorType": "sensor_type",
    "sensorId": "sensor_id",
    "kind": "kind",
    "operator": "operator",
    "threshold": "threshold",
    "windowSeconds": "window_seconds",
    "aggregation": "aggregation",
    "minSamples": "min_samples",
    "enabled": "enabled",
}
COLUMNS = list(FIELDS.values())
MAX_WINDOW_SECONDS = 86400


def _is_text(value):
    return isinstance(value, str) and bool(value.strip())


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _from_body(body):
    return {column: body[field] for field, column in FIELDS.items() if field in body}


def _validate(fields):
    rule = {column: fields.get(column) for column in COLUMNS}
    if rule["enabled"] is None:
        rule["enabled"] = True
    errors = {}
    if not _is_text(rule["name"]):
        errors["name"] = "required non-empty string"
    if not _is_text(rule["sensor_type"]):
        errors["sensorType"] = "required non-empty string"
    if rule["sensor_id"] is not None and not _is_text(rule["sensor_id"]):
        errors["sensorId"] = "must be a non-empty string or null"
    if rule["kind"] not in ("single", "window"):
        errors["kind"] = "must be 'single' or 'window'"
    if rule["operator"] not in OPERATORS:
        errors["operator"] = f"must be one of {sorted(OPERATORS)}"
    threshold = rule["threshold"]
    if isinstance(threshold, bool) or not isinstance(threshold, (int, float)) or not math.isfinite(threshold):
        errors["threshold"] = "must be a finite number"
    if not isinstance(rule["enabled"], bool):
        errors["enabled"] = "must be a boolean"

    if rule["kind"] == "window":
        seconds = rule["window_seconds"]
        if not _is_int(seconds) or not 1 <= seconds <= MAX_WINDOW_SECONDS:
            errors["windowSeconds"] = f"must be an integer between 1 and {MAX_WINDOW_SECONDS}"
        if rule["aggregation"] not in AGGREGATIONS:
            errors["aggregation"] = f"must be one of {sorted(AGGREGATIONS)}"
        if rule["min_samples"] is None:
            rule["min_samples"] = 1
        if not _is_int(rule["min_samples"]) or rule["min_samples"] < 1:
            errors["minSamples"] = "must be a positive integer"
    else:
        rule.update(window_seconds=None, aggregation=None, min_samples=1)

    if errors:
        response = jsonify(error="invalid rule", fields=errors)
        response.status_code = 400
        abort(response)
    return rule


def _get_or_404(rule_id):
    row = db().fetch_one("SELECT * FROM alert_rules WHERE id = %s", (rule_id,))
    if row is None:
        abort(404, "rule not found")
    return row


def _changed(row, status=200):
    ingestor().request_reload()
    return jsonify(to_camel(row)), status


@bp.post("/rules")
def create_rule():
    rule = _validate(_from_body(json_body()))
    row = db().fetch_one(
        f"INSERT INTO alert_rules ({', '.join(COLUMNS)}) "
        f"VALUES ({', '.join(['%s'] * len(COLUMNS))}) RETURNING *",
        [rule[column] for column in COLUMNS],
    )
    return _changed(row, 201)


@bp.get("/rules")
def list_rules():
    clauses, params = [], []
    enabled = request.args.get("enabled")
    if enabled is not None:
        if enabled not in ("true", "false"):
            abort(400, "enabled must be true or false")
        clauses.append("enabled = %s")
        params.append(enabled == "true")
    if request.args.get("sensorType"):
        clauses.append("sensor_type = %s")
        params.append(request.args["sensorType"])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = db().fetch_all(f"SELECT * FROM alert_rules {where} ORDER BY id", params)
    return jsonify(items=[to_camel(row) for row in rows])


@bp.get("/rules/<int:rule_id>")
def get_rule(rule_id):
    return jsonify(to_camel(_get_or_404(rule_id)))


@bp.route("/rules/<int:rule_id>", methods=["PUT", "PATCH"])
def edit_rule(rule_id):
    current = _get_or_404(rule_id)
    rule = _validate({**{column: current[column] for column in COLUMNS}, **_from_body(json_body())})
    row = db().fetch_one(
        f"UPDATE alert_rules SET {', '.join(f'{column} = %s' for column in COLUMNS)}, "
        f"updated_at = now() WHERE id = %s RETURNING *",
        [rule[column] for column in COLUMNS] + [rule_id],
    )
    if row is None:
        abort(404, "rule not found")
    return _changed(row)


def _set_enabled(rule_id, enabled):
    row = db().fetch_one(
        "UPDATE alert_rules SET enabled = %s, updated_at = now() WHERE id = %s RETURNING *",
        (enabled, rule_id),
    )
    if row is None:
        abort(404, "rule not found")
    return _changed(row)


@bp.post("/rules/<int:rule_id>/enable")
def enable_rule(rule_id):
    return _set_enabled(rule_id, True)


@bp.post("/rules/<int:rule_id>/disable")
def disable_rule(rule_id):
    return _set_enabled(rule_id, False)
