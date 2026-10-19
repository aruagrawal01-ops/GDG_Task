import threading

import pytest
from werkzeug.serving import make_server

from conftest import create_member, create_workshop, outbox_rows
from mock_notifier.notifier import create_app as create_notifier
from outbox_worker.worker import http_sender

PAYLOAD = {"event": "registration_confirmed", "workshopId": 1, "memberId": 2, "email": "a@campus.edu"}


@pytest.fixture
def notifier_db(tmp_path):
    return str(tmp_path / "notifier.sqlite3")


@pytest.fixture
def notifier(notifier_db):
    return create_notifier(notifier_db, failure_rate=0, timeout_rate=0, timeout_seconds=0.6)


def send(client, key="confirmation:1"):
    return client.post("/notify", json=PAYLOAD, headers={"Idempotency-Key": key})


def test_delivers_once_and_ignores_duplicates(notifier, notifier_db):
    client = notifier.test_client()
    assert send(client).get_json() == {"status": "delivered"}
    assert send(client).get_json() == {"status": "duplicate"}
    assert client.post("/notify", json=PAYLOAD).status_code == 400

    restarted = create_notifier(notifier_db, failure_rate=0, timeout_rate=0).test_client()
    assert send(restarted).get_json() == {"status": "duplicate"}
    [delivery] = restarted.get("/deliveries").get_json()["items"]
    assert delivery["duplicates"] == 2


def test_injected_failures_and_downtime_do_not_deliver(notifier):
    client = notifier.test_client()
    client.post("/admin/config", json={"failNext": 1})
    assert send(client).status_code == 500
    client.post("/admin/config", json={"down": True})
    assert send(client).status_code == 503
    assert client.get("/deliveries").get_json()["items"] == []
    client.post("/admin/config", json={"down": False})
    assert send(client).status_code == 200


def test_random_failures_follow_the_configured_rate(notifier_db):
    client = create_notifier(notifier_db, failure_rate=1.0, timeout_rate=0).test_client()
    assert {send(client, f"k{i}").status_code for i in range(5)} == {500}


@pytest.fixture
def live_notifier(notifier):
    server = make_server("127.0.0.1", 0, notifier, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", notifier.test_client()
    server.shutdown()


def test_lost_acknowledgement_is_retried_and_deduplicated(client, db, make_worker, live_notifier):
    url, notifier_client = live_notifier
    workshop = create_workshop(client)
    member = create_member(client, "Asha")
    client.post(f"/workshops/{workshop['id']}/register", json={"memberId": member["id"]})
    notifier_client.post("/admin/config", json={"timeoutNext": 1})

    worker = make_worker(http_sender(f"{url}/notify", connect_timeout=1, read_timeout=0.2))
    worker.run_once()
    [row] = outbox_rows(db)
    assert (row["status"], row["last_error"]) == ("pending", "ReadTimeout")

    worker.run_once()
    [row] = outbox_rows(db)
    assert (row["status"], row["attempts"]) == ("sent", 2)
    [delivery] = notifier_client.get("/deliveries").get_json()["items"]
    assert (delivery["idempotencyKey"], delivery["duplicates"]) == (row["idempotency_key"], 1)


def test_unreachable_notifier_is_a_retryable_failure(client, db, make_worker):
    workshop = create_workshop(client)
    client.post(f"/workshops/{workshop['id']}/register", json={"memberId": create_member(client, "Asha")["id"]})
    make_worker(http_sender("http://127.0.0.1:9/notify", connect_timeout=0.2, read_timeout=0.2)).run_once()
    [row] = outbox_rows(db)
    assert (row["status"], row["last_error"]) == ("pending", "ConnectionError")
