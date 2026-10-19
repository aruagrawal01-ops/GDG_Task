"""Mock notification service. A 2xx response means the notification was delivered.

It can fail with HTTP 500, reply only after the caller's timeout (after it has already delivered),
or be switched off. Delivered idempotency keys are stored in SQLite, so a repeated key is
acknowledged again but never delivered twice, even after the notifier restarts.
"""
import json
import logging
import os
import random
import sqlite3
import threading
import time
from datetime import datetime, timezone

from flask import Flask, jsonify, request

log = logging.getLogger("notifier")

SETTINGS = {"failureRate": float, "timeoutRate": float, "timeoutSeconds": float,
            "down": bool, "failNext": int, "timeoutNext": int}


def create_app(db_path=None, failure_rate=None, timeout_rate=None, timeout_seconds=None):
    app = Flask(__name__)
    lock = threading.Lock()
    settings = {
        "failureRate": failure_rate if failure_rate is not None else float(os.getenv("NOTIFIER_FAILURE_RATE", 0.2)),
        "timeoutRate": timeout_rate if timeout_rate is not None else float(os.getenv("NOTIFIER_TIMEOUT_RATE", 0.15)),
        "timeoutSeconds": timeout_seconds if timeout_seconds is not None else float(
            os.getenv("NOTIFIER_TIMEOUT_SECONDS", 5)),
        "down": False,
        "failNext": 0,
        "timeoutNext": 0,
    }
    counters = {"delivered": 0, "duplicatesIgnored": 0, "injectedFailures": 0, "injectedTimeouts": 0,
                "rejectedWhileDown": 0}

    store = sqlite3.connect(db_path or os.getenv("NOTIFIER_DB", "notifier.sqlite3"), check_same_thread=False)
    store.execute(
        """CREATE TABLE IF NOT EXISTS deliveries (
               idempotency_key TEXT PRIMARY KEY,
               event TEXT, workshop_id INTEGER, member_id INTEGER, email TEXT,
               payload TEXT NOT NULL, delivered_at TEXT NOT NULL,
               duplicates INTEGER NOT NULL DEFAULT 0)"""
    )
    store.commit()

    def choose_outcome():
        with lock:
            if settings["down"]:
                return "down"
            if settings["failNext"] > 0:
                settings["failNext"] -= 1
                return "fail"
            if settings["timeoutNext"] > 0:
                settings["timeoutNext"] -= 1
                return "timeout"
            roll = random.random()
            if roll < settings["failureRate"]:
                return "fail"
            if roll < settings["failureRate"] + settings["timeoutRate"]:
                return "timeout"
            return "ok"

    def record(key, payload):
        with lock:
            cursor = store.execute(
                "INSERT OR IGNORE INTO deliveries (idempotency_key, event, workshop_id, member_id, email, "
                "payload, delivered_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (key, payload.get("event"), payload.get("workshopId"), payload.get("memberId"),
                 payload.get("email"), json.dumps(payload), datetime.now(timezone.utc).isoformat()),
            )
            duplicate = cursor.rowcount == 0
            if duplicate:
                store.execute("UPDATE deliveries SET duplicates = duplicates + 1 WHERE idempotency_key = ?", (key,))
                counters["duplicatesIgnored"] += 1
            else:
                counters["delivered"] += 1
            store.commit()
            return duplicate

    def bump(counter):
        with lock:
            counters[counter] += 1

    @app.post("/notify")
    def notify():
        key = request.headers.get("Idempotency-Key")
        if not key:
            return jsonify(error="missing Idempotency-Key header"), 400
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(error="body must be a JSON object"), 400

        outcome = choose_outcome()
        if outcome == "down":
            bump("rejectedWhileDown")
            return jsonify(error="notifier is down"), 503
        if outcome == "fail":
            bump("injectedFailures")
            log.info("FAILED (injected 500) %s", key)
            return jsonify(error="simulated failure"), 500

        duplicate = record(key, payload)
        log.info("%s %s -> %s", "DUPLICATE-IGNORED" if duplicate else "DELIVERED", key, payload.get("email"))
        if outcome == "timeout":
            bump("injectedTimeouts")
            log.info("SLOW REPLY %s (already processed, answering in %.0fs)", key, settings["timeoutSeconds"])
            time.sleep(settings["timeoutSeconds"])
        return jsonify(status="duplicate" if duplicate else "delivered")

    @app.get("/deliveries")
    def deliveries():
        clauses, params = [], []
        for arg, column in (("workshopId", "workshop_id"), ("memberId", "member_id")):
            if request.args.get(arg, "").isdigit():
                clauses.append(f"{column} = ?")
                params.append(int(request.args[arg]))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with lock:
            rows = store.execute(
                f"SELECT idempotency_key, event, workshop_id, member_id, email, delivered_at, duplicates "
                f"FROM deliveries {where} ORDER BY delivered_at, rowid",
                params,
            ).fetchall()
        fields = ("idempotencyKey", "event", "workshopId", "memberId", "email", "deliveredAt", "duplicates")
        return jsonify(items=[dict(zip(fields, row)) for row in rows])

    @app.get("/stats")
    def stats():
        with lock:
            return jsonify(settings=dict(settings), counters=dict(counters))

    @app.post("/admin/config")
    def configure():
        body = request.get_json(silent=True) or {}
        try:
            updates = {name: kind(body[name]) for name, kind in SETTINGS.items() if name in body}
        except (TypeError, ValueError):
            return jsonify(error=f"settings must be {', '.join(SETTINGS)} with numeric or boolean values"), 400
        with lock:
            settings.update(updates)
            return jsonify(settings=dict(settings))

    @app.post("/admin/reset")
    def reset():
        with lock:
            store.execute("DELETE FROM deliveries")
            store.commit()
            for name in counters:
                counters[name] = 0
        return "", 204

    return app
