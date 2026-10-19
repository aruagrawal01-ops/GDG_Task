from datetime import datetime

from flask import abort, current_app, request


def db():
    return current_app.extensions["db"]


def json_body():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        abort(400, "request body must be a JSON object")
    return body


def int_arg(name, default=None, minimum=1, maximum=None):
    raw = request.args.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        abort(400, f"{name} must be an integer")
    if value < minimum:
        abort(400, f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        abort(400, f"{name} must be at most {maximum}")
    return value


def to_camel(row):
    result = {}
    for key, value in row.items():
        head, *rest = key.split("_")
        name = head + "".join(part.title() for part in rest)
        result[name] = value.isoformat() if isinstance(value, datetime) else value
    return result
