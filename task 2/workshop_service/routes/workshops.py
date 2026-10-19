from datetime import datetime, timezone

from flask import Blueprint, abort, jsonify

from ..registration import RegistrationError, cancel, register
from . import db, int_arg, json_body, to_camel

bp = Blueprint("workshops", __name__)

WORKSHOP_SQL = """
SELECT w.*, (SELECT count(*) FROM registrations r WHERE r.workshop_id = w.id) AS registered
FROM workshops w
"""


def _get_or_404(workshop_id):
    row = db().fetch_one(f"{WORKSHOP_SQL} WHERE w.id = %s", (workshop_id,))
    if row is None:
        abort(404, "workshop not found")
    return row


@bp.post("/workshops")
def create_workshop():
    body = json_body()
    title = body.get("title")
    if not isinstance(title, str) or not title.strip() or len(title) > 200:
        abort(400, "title is required (at most 200 characters)")

    starts_at = body.get("startsAt")
    if starts_at is not None:
        try:
            starts_at = datetime.fromisoformat(starts_at)
        except (TypeError, ValueError):
            abort(400, "startsAt must be an ISO 8601 string")
        if starts_at.tzinfo is None:
            starts_at = starts_at.replace(tzinfo=timezone.utc)

    capacity = body.get("capacity")
    if capacity is not None and (not isinstance(capacity, int) or isinstance(capacity, bool) or capacity < 1):
        abort(400, "capacity must be a positive integer")

    row = db().fetch_one(
        "INSERT INTO workshops (title, starts_at, capacity) VALUES (%s, %s, %s) RETURNING id",
        (title.strip(), starts_at, capacity),
    )
    return jsonify(to_camel(_get_or_404(row["id"]))), 201


@bp.get("/workshops")
def list_workshops():
    limit = int_arg("limit", default=100, maximum=1000)
    rows = db().fetch_all(f"{WORKSHOP_SQL} ORDER BY w.id DESC LIMIT %s", (limit,))
    return jsonify(items=[to_camel(row) for row in rows])


@bp.get("/workshops/<int:workshop_id>")
def get_workshop(workshop_id):
    return jsonify(to_camel(_get_or_404(workshop_id)))


@bp.post("/workshops/<int:workshop_id>/register")
def register_member(workshop_id):
    member_id = json_body().get("memberId")
    if not isinstance(member_id, int) or isinstance(member_id, bool):
        abort(400, "memberId must be an integer")
    try:
        result = register(db(), workshop_id, member_id)
    except RegistrationError as exc:
        abort(exc.status, exc.message)
    return jsonify(result), 201


@bp.get("/workshops/<int:workshop_id>/registrations")
def list_registrations(workshop_id):
    _get_or_404(workshop_id)
    rows = db().fetch_all(
        """
        SELECT r.id AS registration_id, r.member_id, m.name, m.email, r.created_at,
               c.status AS confirmation_status, x.status AS cancellation_status
        FROM registrations r
        JOIN members m ON m.id = r.member_id
        LEFT JOIN outbox c ON c.idempotency_key = 'confirmation:' || r.id
        LEFT JOIN outbox x ON x.idempotency_key = 'cancellation:' || r.id
        WHERE r.workshop_id = %s
        ORDER BY r.id
        """,
        (workshop_id,),
    )
    return jsonify(items=[to_camel(row) for row in rows])


@bp.post("/workshops/<int:workshop_id>/cancel")
def cancel_workshop(workshop_id):
    try:
        workshop, queued, already_cancelled = cancel(db(), workshop_id)
    except RegistrationError as exc:
        abort(exc.status, exc.message)
    return jsonify(workshop=to_camel(workshop), notificationsQueued=queued, alreadyCancelled=already_cancelled)
