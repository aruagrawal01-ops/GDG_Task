import time

from conftest import TEST_DATABASE_URL, create_member, create_workshop, outbox_rows
from outbox_worker.worker import OutboxWorker
from workshop_service import create_app


def register_members(client, *names, **workshop_fields):
    workshop = create_workshop(client, **workshop_fields)
    for name in names:
        client.post(f"/workshops/{workshop['id']}/register", json={"memberId": create_member(client, name)["id"]})
    return workshop


def succeed(row):
    return True, None


def test_worker_delivers_pending_confirmations(client, db, make_worker):
    register_members(client, "Asha", "Ben")
    delivered = []
    worker = make_worker(lambda row: (delivered.append(row["idempotency_key"]) or True, None))

    assert worker.run_once() == 2
    assert sorted(delivered) == ["confirmation:1", "confirmation:2"]
    assert [(r["status"], r["attempts"]) for r in outbox_rows(db)] == [("sent", 1), ("sent", 1)]
    assert worker.run_once() == 0


def test_failure_is_retried_with_backoff(client, db, make_worker):
    register_members(client, "Asha")
    worker = make_worker(lambda row: (False, "HTTP 500"), backoff_base=60, backoff_cap=60)

    worker.run_once()
    [row] = outbox_rows(db)
    assert (row["status"], row["attempts"], row["last_error"]) == ("pending", 1, "HTTP 500")
    assert row["next_attempt_at"] > db.fetch_one("SELECT now() AS now")["now"]
    assert worker.run_once() == 0


def test_one_failing_member_does_not_block_others(client, db, make_worker):
    register_members(client, "Asha", "Ben", "Chen")
    worker = make_worker(lambda row: (row["payload"]["memberName"] != "Ben", "HTTP 500"))

    for _ in range(3):
        worker.run_once()
    statuses = {r["payload"]["memberName"]: (r["status"], r["attempts"]) for r in outbox_rows(db)}
    assert statuses == {"Asha": ("sent", 1), "Ben": ("pending", 3), "Chen": ("sent", 1)}


def test_exhausted_retries_become_dead_and_can_be_replayed(client, db, make_worker):
    register_members(client, "Asha")
    worker = make_worker(lambda row: (False, "ConnectionError"), max_attempts=3)
    for _ in range(5):
        worker.run_once()
    [row] = outbox_rows(db)
    assert (row["status"], row["attempts"]) == ("dead", 3)

    replay = client.post(f"/outbox/{row['id']}/retry")
    assert replay.get_json()["status"] == "pending"
    make_worker(succeed).run_once()
    assert outbox_rows(db)[0]["status"] == "sent"
    assert client.post(f"/outbox/{row['id']}/retry").status_code == 409
    assert client.post("/outbox/999/retry").status_code == 404


def test_sender_exception_counts_as_failure(client, db, make_worker):
    register_members(client, "Asha")

    def explode(row):
        raise RuntimeError("bug in sender")

    make_worker(explode).run_once()
    [row] = outbox_rows(db)
    assert row["status"] == "pending" and "bug in sender" in row["last_error"]


def test_cancellation_waits_for_the_confirmation(client, db, make_worker):
    workshop = register_members(client, "Asha")
    client.post(f"/workshops/{workshop['id']}/cancel")
    seen = []
    failing = make_worker(lambda row: (seen.append(row["kind"]) or False, "HTTP 500"))

    failing.run_once()
    failing.run_once()
    assert seen == ["confirmation", "confirmation"]

    make_worker(lambda row: (seen.append(row["kind"]) or True, None)).run_once()
    make_worker(lambda row: (seen.append(row["kind"]) or True, None)).run_once()
    assert seen[2:] == ["confirmation", "cancellation"]
    assert {r["status"] for r in outbox_rows(db)} == {"sent"}


def test_dead_confirmation_does_not_block_cancellation(client, db, make_worker):
    workshop = register_members(client, "Asha")
    client.post(f"/workshops/{workshop['id']}/cancel")
    make_worker(lambda row: (False, "HTTP 500"), max_attempts=1).run_once()
    make_worker(succeed).run_once()
    assert [(r["kind"], r["status"]) for r in outbox_rows(db)] == [("confirmation", "dead"), ("cancellation", "sent")]


def test_expired_lease_is_reclaimed_and_stale_result_ignored(client, db, make_worker):
    register_members(client, "Asha")
    crashed = make_worker(succeed, lease_seconds=1)
    [claimed] = crashed.claim(10)

    other = make_worker(succeed, lease_seconds=30)
    assert other.claim(10) == []
    time.sleep(1.2)
    [reclaimed] = other.claim(10)
    assert reclaimed["attempts"] == 2

    crashed.finish(claimed, False, "late failure from the crashed worker")
    assert outbox_rows(db)[0]["last_error"] is None
    other.finish(reclaimed, True, None)
    assert outbox_rows(db)[0]["status"] == "sent"


def test_rows_survive_a_restart_of_api_and_worker(client, db, make_worker):
    register_members(client, "Asha")
    first = make_worker(lambda row: (False, "ConnectionError"))
    first.run_once()

    restarted = create_app({"DATABASE_URL": TEST_DATABASE_URL})
    try:
        OutboxWorker(restarted.extensions["db"], succeed, backoff_base=0, backoff_cap=0).run_once()
        assert [(r["status"], r["attempts"]) for r in outbox_rows(db)] == [("sent", 2)]
    finally:
        restarted.extensions["db"].close()


def test_stats_and_outbox_views(client, make_worker):
    register_members(client, "Asha", "Ben")
    make_worker(lambda row: (row["payload"]["memberName"] == "Asha", "HTTP 500"), backoff_base=60,
                backoff_cap=60).run_once()
    stats = client.get("/stats").get_json()
    assert stats["outbox"]["confirmation"]["sent"]["count"] == 1
    assert stats["outbox"]["confirmation"]["pending"]["count"] == 1
    assert stats["oldestPendingSeconds"] >= 0
    pending = client.get("/outbox", query_string={"status": "pending"}).get_json()["items"]
    assert [r["lastError"] for r in pending] == ["HTTP 500"]
    assert client.get("/outbox", query_string={"status": "lost"}).status_code == 400
