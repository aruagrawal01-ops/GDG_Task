from datetime import datetime, timedelta, timezone

from psycopg2 import OperationalError

from conftest import make_app, reading, reset_database, shutdown, wait_for_flush
from sensor_service import ingest

SINGLE_RULE = {"name": "hot", "sensorType": "temperature", "kind": "single", "operator": ">", "threshold": 30}
WINDOW_RULE = {"name": "avg hot", "sensorType": "temperature", "kind": "window", "operator": ">",
               "threshold": 28, "windowSeconds": 600, "aggregation": "avg"}


def post_readings(client, app, payload):
    response = client.post("/readings", json=payload)
    wait_for_flush(app)
    return response


def alerts(client, **params):
    return client.get("/alerts", query_string=params).get_json()["items"]


def test_accepts_single_reading_and_batch(client, app):
    assert post_readings(client, app, reading(21.0)).status_code == 202
    response = post_readings(client, app, [reading(22.0), reading(23.0, sensor="lab-1")])
    assert response.get_json() == {"accepted": 2, "rejected": []}
    stored = client.get("/readings", query_string={"sensorId": "lab-1"}).get_json()["items"]
    assert [(r["sensorId"], r["value"], r["timestamp"]) for r in stored] == [("lab-1", 23.0, "2026-10-12T10:30:00+00:00")]


def test_rejects_invalid_items_but_keeps_valid_ones(client, app):
    response = post_readings(client, app, [reading(21.0), {"sensorId": "x", "type": "t", "value": "hot"},
                                           reading(True), reading(1, timestamp="yesterday")])
    body = response.get_json()
    assert response.status_code == 202
    assert body["accepted"] == 1
    assert [e["index"] for e in body["rejected"]] == [1, 2, 3]


def test_rejects_bad_requests(client, app):
    assert client.post("/readings", data="nope", content_type="application/json").status_code == 400
    assert client.post("/readings", json=[]).status_code == 400
    assert client.post("/readings", json=[{"sensorId": ""}]).status_code == 400
    too_many = [reading(1)] * (app.config["MAX_BATCH_SIZE"] + 1)
    assert client.post("/readings", json=too_many).status_code == 413


def test_full_queue_returns_429_and_never_accepts_part_of_a_batch():
    reset_database()
    app = make_app(START_CONSUMER=False, QUEUE_CAPACITY=5)
    try:
        client = app.test_client()
        assert client.post("/readings", json=[reading(1)] * 4).status_code == 202
        response = client.post("/readings", json=[reading(1)] * 2)
        assert response.status_code == 429
        assert response.headers["Retry-After"] == "1"
        assert app.extensions["ingestor"].queue.depth == 4
    finally:
        shutdown(app)


def test_rule_crud_and_validation(client):
    bad = client.post("/rules", json={"name": "", "kind": "window", "operator": "=="})
    assert bad.status_code == 400
    assert {"name", "sensorType", "operator", "threshold", "windowSeconds", "aggregation"} <= set(
        bad.get_json()["fields"])

    created = client.post("/rules", json=WINDOW_RULE)
    assert created.status_code == 201
    rule = created.get_json()
    assert (rule["kind"], rule["minSamples"], rule["enabled"]) == ("window", 1, True)

    edited = client.put(f"/rules/{rule['id']}", json={"threshold": 26.5, "minSamples": 3}).get_json()
    assert (edited["threshold"], edited["minSamples"], edited["windowSeconds"]) == (26.5, 3, 600)
    assert client.patch(f"/rules/{rule['id']}", json={"aggregation": "median"}).status_code == 400

    assert client.post(f"/rules/{rule['id']}/disable").get_json()["enabled"] is False
    assert client.get("/rules", query_string={"enabled": "false"}).get_json()["items"][0]["id"] == rule["id"]
    assert client.post(f"/rules/{rule['id']}/enable").get_json()["enabled"] is True
    assert client.get("/rules/999").status_code == 404
    assert client.post("/rules/999/enable").status_code == 404


def test_single_rule_creates_one_alert_per_active_breach(client, app):
    client.post("/rules", json=SINGLE_RULE)
    post_readings(client, app, reading(34.5))
    post_readings(client, app, reading(36.0))
    post_readings(client, app, reading(20.0))

    [alert] = alerts(client)
    assert (alert["status"], alert["triggeredValue"], alert["lastValue"], alert["triggerCount"]) == (
        "open", 34.5, 36.0, 2)
    assert alert["message"] == "temperature 36 > 30"


def test_alert_lifecycle(client, app):
    client.post("/rules", json=SINGLE_RULE)
    post_readings(client, app, reading(34.5))
    [alert] = alerts(client)

    acknowledged = client.post(f"/alerts/{alert['id']}/acknowledge")
    assert acknowledged.get_json()["status"] == "acknowledged"
    assert client.post(f"/alerts/{alert['id']}/acknowledge").status_code == 409

    post_readings(client, app, reading(35.0))
    assert len(alerts(client)) == 1

    resolved = client.post(f"/alerts/{alert['id']}/resolve").get_json()
    assert resolved["status"] == "resolved" and resolved["resolvedAt"]
    assert client.post(f"/alerts/{alert['id']}/resolve").status_code == 409
    assert client.post(f"/alerts/{alert['id']}/acknowledge").status_code == 409
    assert client.post("/alerts/999/resolve").status_code == 404

    post_readings(client, app, reading(35.0))
    assert [a["status"] for a in alerts(client)] == ["open", "resolved"]


def test_disabled_rule_does_not_alert(client, app):
    rule = client.post("/rules", json=SINGLE_RULE).get_json()
    client.post(f"/rules/{rule['id']}/disable")
    post_readings(client, app, reading(50.0))
    assert alerts(client) == []

    client.post(f"/rules/{rule['id']}/enable")
    post_readings(client, app, reading(50.0))
    assert len(alerts(client)) == 1


def test_window_rule_fires_once(client, app):
    client.post("/rules", json={**WINDOW_RULE, "minSamples": 3})
    post_readings(client, app, reading(30, timestamp="2026-10-12T10:00:00Z"))
    post_readings(client, app, reading(31, timestamp="2026-10-12T10:01:00Z"))
    assert alerts(client) == []
    post_readings(client, app, reading(32, timestamp="2026-10-12T10:02:00Z"))
    post_readings(client, app, reading(33, timestamp="2026-10-12T10:03:00Z"))

    [alert] = alerts(client)
    assert alert["triggeredValue"] == 31.0
    assert alert["lastValue"] == 31.5
    assert alert["triggerCount"] == 2


def test_window_state_is_rebuilt_after_restart(client, app):
    client.post("/rules", json={**WINDOW_RULE, "minSamples": 4})
    now = datetime.now(timezone.utc)
    stamps = [(now - timedelta(seconds=s)).isoformat() for s in (30, 20, 10)]
    post_readings(client, app, [reading(40, timestamp=t) for t in stamps])
    assert alerts(client) == []
    shutdown(app)

    restarted = make_app()
    try:
        restarted_client = restarted.test_client()
        post_readings(restarted_client, restarted, reading(40, timestamp=now.isoformat()))
        [alert] = alerts(restarted_client)
        assert "4 samples" in alert["message"]
    finally:
        shutdown(restarted)


def test_stop_drains_queued_readings():
    reset_database()
    app = make_app(FLUSH_WAIT_MS=200)
    client = app.test_client()
    for _ in range(10):
        client.post("/readings", json=[reading(1)] * 100)
    shutdown(app)

    check = make_app(START_CONSUMER=False)
    try:
        assert check.extensions["db"].fetch_one("SELECT count(*) AS n FROM readings")["n"] == 1000
        assert check.test_client().post("/readings", json=reading(1)).status_code == 202
    finally:
        shutdown(check)


def test_alert_filters_and_pagination(client, app):
    client.post("/rules", json=SINGLE_RULE)
    post_readings(client, app, [reading(40, sensor=f"s{i}") for i in range(5)])
    page = client.get("/alerts", query_string={"limit": 2}).get_json()
    assert len(page["items"]) == 2
    rest = client.get("/alerts", query_string={"limit": 10, "beforeId": page["nextBeforeId"]}).get_json()
    assert len(rest["items"]) == 3 and rest["nextBeforeId"] is None
    assert [a["sensorId"] for a in alerts(client, sensorId="s3")] == ["s3"]
    assert alerts(client, status="resolved") == []
    assert client.get("/alerts", query_string={"status": "closed"}).status_code == 400
    assert client.get("/alerts", query_string={"limit": "x"}).status_code == 400


def test_health_and_stats(client, app):
    assert client.get("/health").get_json()["status"] == "ok"
    post_readings(client, app, [reading(1)] * 3)
    stats = client.get("/stats").get_json()
    assert stats["counters"]["writtenReadings"] == 3
    assert stats["queue"]["depth"] == 0


def test_failed_flush_is_retried_without_losing_or_double_counting(client, app, monkeypatch):
    real_execute_values = ingest.execute_values
    calls = {"n": 0}

    def flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise OperationalError("database went away")
        return real_execute_values(*args, **kwargs)

    now = datetime.now(timezone.utc)
    client.post("/rules", json={**WINDOW_RULE, "minSamples": 2})
    post_readings(client, app, reading(40, timestamp=(now - timedelta(seconds=5)).isoformat()))
    monkeypatch.setattr(ingest, "execute_values", flaky)
    post_readings(client, app, reading(40, timestamp=now.isoformat()))

    counters = app.extensions["ingestor"].stats.snapshot()
    assert counters["failed_flushes"] == 1
    assert counters["written_readings"] == 2
    [alert] = alerts(client)
    assert "2 samples" in alert["message"]
