from flask import Flask, jsonify, request
from psycopg2 import OperationalError
from psycopg2.errors import UniqueViolation

from db import get_cursor

app = Flask(__name__)


def error(message, status):
    return jsonify(error=message), status


def text_field(data, name):
    value = data.get(name) if isinstance(data, dict) else None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


@app.post("/workshops")
def create_workshop():
    title = text_field(request.get_json(silent=True), "title")
    if title is None:
        return error("title must be a non-empty string", 400)
    with get_cursor() as cur:
        cur.execute("INSERT INTO workshops (title) VALUES (%s) RETURNING *", (title,))
        return jsonify(cur.fetchone()), 201


@app.get("/workshops/<int:workshop_id>")
def get_workshop(workshop_id):
    with get_cursor() as cur:
        cur.execute("SELECT * FROM workshops WHERE id = %s", (workshop_id,))
        workshop = cur.fetchone()
        if workshop is None:
            return error("workshop not found", 404)
        cur.execute(
            "SELECT id, member_id, created_at FROM registrations WHERE workshop_id = %s ORDER BY id",
            (workshop_id,),
        )
        workshop["registrations"] = cur.fetchall()
    return jsonify(workshop)


@app.post("/workshops/<int:workshop_id>/register")
def register(workshop_id):
    member_id = text_field(request.get_json(silent=True), "member_id")
    if member_id is None:
        return error("member_id must be a non-empty string", 400)
    try:
        with get_cursor() as cur:
            cur.execute("SELECT status FROM workshops WHERE id = %s FOR SHARE", (workshop_id,))
            workshop = cur.fetchone()
            if workshop is None:
                return error("workshop not found", 404)
            if workshop["status"] == "cancelled":
                return error("workshop is cancelled", 409)
            cur.execute(
                "INSERT INTO registrations (workshop_id, member_id) VALUES (%s, %s) RETURNING id",
                (workshop_id, member_id),
            )
            registration_id = cur.fetchone()["id"]
            cur.execute(
                "INSERT INTO notifications (registration_id, kind) VALUES (%s, 'confirmation')",
                (registration_id,),
            )
    except UniqueViolation:
        return error("member is already registered for this workshop", 409)
    return jsonify(registration_id=registration_id, status="registered"), 201


@app.post("/workshops/<int:workshop_id>/cancel")
def cancel(workshop_id):
    with get_cursor() as cur:
        cur.execute("SELECT status FROM workshops WHERE id = %s FOR UPDATE", (workshop_id,))
        workshop = cur.fetchone()
        if workshop is None:
            return error("workshop not found", 404)
        if workshop["status"] == "cancelled":
            return error("workshop is already cancelled", 409)
        cur.execute("UPDATE workshops SET status = 'cancelled' WHERE id = %s", (workshop_id,))
        cur.execute(
            """
            INSERT INTO notifications (registration_id, kind)
            SELECT id, 'cancellation' FROM registrations WHERE workshop_id = %s
            """,
            (workshop_id,),
        )
        queued = cur.rowcount
    return jsonify(status="cancelled", notifications_queued=queued)


@app.get("/notifications")
def list_notifications():
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT n.id, n.kind, n.status, n.attempts, n.last_error, n.next_attempt_at,
                   r.member_id, r.workshop_id
            FROM notifications n JOIN registrations r ON r.id = n.registration_id
            ORDER BY n.id
            """
        )
        return jsonify(cur.fetchall())


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
    app.run(port=8001, threaded=True)
