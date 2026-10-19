from flask import Blueprint, abort, jsonify, request

from . import db, int_arg, to_camel

bp = Blueprint("outbox", __name__)

STATUSES = ("pending", "sent", "dead")
KINDS = ("confirmation", "cancellation")


@bp.get("/outbox")
def list_outbox():
    clauses, params = [], []
    for arg, allowed in (("status", STATUSES), ("kind", KINDS)):
        value = request.args.get(arg)
        if value is not None:
            if value not in allowed:
                abort(400, f"{arg} must be one of {', '.join(allowed)}")
            clauses.append(f"{arg} = %s")
            params.append(value)
    workshop_id = int_arg("workshopId")
    if workshop_id is not None:
        clauses.append("workshop_id = %s")
        params.append(workshop_id)
    limit = int_arg("limit", default=100, maximum=1000)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = db().fetch_all(
        f"SELECT id, kind, registration_id, workshop_id, member_id, idempotency_key, depends_on, status, "
        f"attempts, next_attempt_at, locked_until, last_error, created_at, sent_at "
        f"FROM outbox {where} ORDER BY id LIMIT %s",
        params + [limit],
    )
    return jsonify(items=[to_camel(row) for row in rows])


@bp.post("/outbox/<int:outbox_id>/retry")
def retry_dead(outbox_id):
    row = db().fetch_one(
        "UPDATE outbox SET status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL "
        "WHERE id = %s AND status = 'dead' RETURNING id, idempotency_key, status, attempts",
        (outbox_id,),
    )
    if row is not None:
        return jsonify(to_camel(row))
    current = db().fetch_one("SELECT status FROM outbox WHERE id = %s", (outbox_id,))
    if current is None:
        abort(404, "outbox row not found")
    abort(409, f"only dead rows can be retried, this one is {current['status']}")
