from flask import Blueprint, abort, jsonify
from psycopg2.errors import UniqueViolation

from . import db, int_arg, json_body, to_camel

bp = Blueprint("members", __name__)


@bp.post("/members")
def create_member():
    body = json_body()
    name, email = body.get("name"), body.get("email")
    if not isinstance(name, str) or not name.strip():
        abort(400, "name is required")
    if not isinstance(email, str) or "@" not in email or len(email) > 254:
        abort(400, "a valid email is required")
    try:
        row = db().fetch_one(
            "INSERT INTO members (name, email) VALUES (%s, %s) RETURNING *",
            (name.strip(), email.strip().lower()),
        )
    except UniqueViolation:
        abort(409, "a member with this email already exists")
    return jsonify(to_camel(row)), 201


@bp.get("/members")
def list_members():
    limit = int_arg("limit", default=100, maximum=1000)
    rows = db().fetch_all("SELECT * FROM members ORDER BY id LIMIT %s", (limit,))
    return jsonify(items=[to_camel(row) for row in rows])


@bp.get("/members/<int:member_id>")
def get_member(member_id):
    row = db().fetch_one("SELECT * FROM members WHERE id = %s", (member_id,))
    if row is None:
        abort(404, "member not found")
    return jsonify(to_camel(row))
