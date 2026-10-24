from datetime import datetime, timezone

from flask import Flask, jsonify, request
from psycopg2 import OperationalError
from psycopg2.extras import execute_values

from db import get_cursor
from rules import OPERATORS, check_rules

MAX_BATCH_SIZE = 5000
ALERT_TRANSITIONS = {
    "acknowledge": ("acknowledged", ["open"]),
    "resolve": ("resolved", ["open", "acknowledged"]),
}

app = Flask(__name__)


def error(message, status):
    return jsonify(error=message), status


def is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def parse_reading(item):
    if not isinstance(item, dict):
        return None, "reading must be a JSON object"
    sensor_id, reading_type, value = item.get("sensorId"), item.get("type"), item.get("value")
    if not isinstance(sensor_id, str) or not sensor_id.strip():
        return None, "sensorId must be a non-empty string"
    if not isinstance(reading_type, str) or not reading_type.strip():
        return None, "type must be a non-empty string"
    if not is_number(value):
        return None, "value must be a number"
    try:
        timestamp = datetime.fromisoformat(item.get("timestamp"))
    except (TypeError, ValueError):
        return None, "timestamp must be an ISO 8601 date, e.g. 2026-10-12T10:30:00Z"
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return {"sensor_id": sensor_id, "type": reading_type, "value": float(value), "timestamp": timestamp}, None


def parse_rule(data):
    if not isinstance(data, dict):
        return None, "body must be a JSON object"
    name, rule_type = data.get("name"), data.get("type")
    operator, threshold = data.get("operator"), data.get("threshold")
    window_minutes = data.get("window_minutes")
    if not isinstance(name, str) or not name.strip():
        return None, "name must be a non-empty string"
    if not isinstance(rule_type, str) or not rule_type.strip():
        return None, "type must be a non-empty string"
    if operator not in OPERATORS:
        return None, f"operator must be one of {', '.join(OPERATORS)}"
    if not is_number(threshold):
        return None, "threshold must be a number"
    if window_minutes is not None and (not isinstance(window_minutes, int) or window_minutes < 1):
        return None, "window_minutes must be a positive whole number, or left out for a single-reading rule"
    return {"name": name, "type": rule_type, "operator": operator,
            "threshold": threshold, "window_minutes": window_minutes}, None


@app.post("/readings")
def add_readings():
    data = request.get_json(silent=True)
    if data is None:
        return error("body must be JSON", 400)
    items = data if isinstance(data, list) else [data]
    if not items:
        return error("no readings sent", 400)
    if len(items) > MAX_BATCH_SIZE:
        return error(f"send at most {MAX_BATCH_SIZE} readings per request", 413)

    readings = []
    for index, item in enumerate(items):
        reading, problem = parse_reading(item)
        if problem:
            return jsonify(error=problem, index=index), 400
        readings.append(reading)

    with get_cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO readings (sensor_id, type, value, timestamp) VALUES %s",
            [(r["sensor_id"], r["type"], r["value"], r["timestamp"]) for r in readings],
            page_size=1000,
        )
        alerts_created = check_rules(cur, readings)
    return jsonify(saved=len(readings), alerts_created=alerts_created), 201


@app.post("/rules")
def create_rule():
    rule, problem = parse_rule(request.get_json(silent=True))
    if problem:
        return error(problem, 400)
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO rules (name, type, operator, threshold, window_minutes)
            VALUES (%(name)s, %(type)s, %(operator)s, %(threshold)s, %(window_minutes)s)
            RETURNING *
            """,
            rule,
        )
        return jsonify(cur.fetchone()), 201


@app.get("/rules")
def list_rules():
    with get_cursor() as cur:
        cur.execute("SELECT * FROM rules ORDER BY id")
        return jsonify(cur.fetchall())


@app.put("/rules/<int:rule_id>")
def edit_rule(rule_id):
    rule, problem = parse_rule(request.get_json(silent=True))
    if problem:
        return error(problem, 400)
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE rules SET name = %(name)s, type = %(type)s, operator = %(operator)s,
                threshold = %(threshold)s, window_minutes = %(window_minutes)s
            WHERE id = %(id)s RETURNING *
            """,
            {**rule, "id": rule_id},
        )
        updated = cur.fetchone()
    if updated is None:
        return error("rule not found", 404)
    return jsonify(updated)


@app.post("/rules/<int:rule_id>/<action>")
def enable_or_disable_rule(rule_id, action):
    if action not in ("enable", "disable"):
        return error("action must be enable or disable", 404)
    with get_cursor() as cur:
        cur.execute("UPDATE rules SET enabled = %s WHERE id = %s RETURNING *", (action == "enable", rule_id))
        updated = cur.fetchone()
    if updated is None:
        return error("rule not found", 404)
    return jsonify(updated)


@app.get("/alerts")
def list_alerts():
    status = request.args.get("status")
    with get_cursor() as cur:
        if status:
            cur.execute("SELECT * FROM alerts WHERE status = %s ORDER BY id DESC", (status,))
        else:
            cur.execute("SELECT * FROM alerts ORDER BY id DESC")
        return jsonify(cur.fetchall())


@app.post("/alerts/<int:alert_id>/<action>")
def change_alert(alert_id, action):
    if action not in ALERT_TRANSITIONS:
        return error("action must be acknowledge or resolve", 404)
    new_status, allowed_from = ALERT_TRANSITIONS[action]
    with get_cursor() as cur:
        cur.execute(
            "UPDATE alerts SET status = %s WHERE id = %s AND status = ANY(%s) RETURNING *",
            (new_status, alert_id, allowed_from),
        )
        updated = cur.fetchone()
        if updated is None:
            cur.execute("SELECT status FROM alerts WHERE id = %s", (alert_id,))
            current = cur.fetchone()
    if updated:
        return jsonify(updated)
    if current is None:
        return error("alert not found", 404)
    return error(f"cannot {action} an alert that is already {current['status']}", 409)


@app.errorhandler(404)
def not_found(_):
    return error("not found", 404)


@app.errorhandler(405)
def method_not_allowed(_):
    return error("method not allowed", 405)


@app.errorhandler(OperationalError)
def database_unavailable(_):
    return error("database unavailable, try again later", 503)


if __name__ == "__main__":
    app.run(port=8000)
