from flask import Blueprint, abort, jsonify, request

from . import db, int_arg, to_camel

bp = Blueprint("alerts", __name__)

STATUSES = ("open", "acknowledged", "resolved")

TRANSITIONS = {
    "acknowledge": (
        "UPDATE alerts SET status = 'acknowledged', acknowledged_at = now() "
        "WHERE id = %s AND status = 'open' RETURNING *"
    ),
    "resolve": (
        "UPDATE alerts SET status = 'resolved', resolved_at = now() "
        "WHERE id = %s AND status IN ('open', 'acknowledged') RETURNING *"
    ),
}


@bp.get("/alerts")
def list_alerts():
    clauses, params = [], []
    status = request.args.get("status")
    if status is not None:
        if status not in STATUSES:
            abort(400, f"status must be one of {', '.join(STATUSES)}")
        clauses.append("status = %s")
        params.append(status)
    if request.args.get("sensorId"):
        clauses.append("sensor_id = %s")
        params.append(request.args["sensorId"])
    rule_id = int_arg("ruleId")
    if rule_id is not None:
        clauses.append("rule_id = %s")
        params.append(rule_id)
    before_id = int_arg("beforeId")
    if before_id is not None:
        clauses.append("id < %s")
        params.append(before_id)

    limit = int_arg("limit", default=50, maximum=500)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = db().fetch_all(f"SELECT * FROM alerts {where} ORDER BY id DESC LIMIT %s", params + [limit])
    next_before = rows[-1]["id"] if len(rows) == limit else None
    return jsonify(items=[to_camel(row) for row in rows], nextBeforeId=next_before)


@bp.get("/alerts/<int:alert_id>")
def get_alert(alert_id):
    row = db().fetch_one("SELECT * FROM alerts WHERE id = %s", (alert_id,))
    if row is None:
        abort(404, "alert not found")
    return jsonify(to_camel(row))


def _transition(alert_id, action):
    row = db().fetch_one(TRANSITIONS[action], (alert_id,))
    if row is not None:
        return jsonify(to_camel(row))
    current = db().fetch_one("SELECT status FROM alerts WHERE id = %s", (alert_id,))
    if current is None:
        abort(404, "alert not found")
    abort(409, f"cannot {action} an alert that is {current['status']}")


@bp.post("/alerts/<int:alert_id>/acknowledge")
def acknowledge_alert(alert_id):
    return _transition(alert_id, "acknowledge")


@bp.post("/alerts/<int:alert_id>/resolve")
def resolve_alert(alert_id):
    return _transition(alert_id, "resolve")
