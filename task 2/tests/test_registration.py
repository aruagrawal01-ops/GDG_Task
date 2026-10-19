import threading
from concurrent.futures import ThreadPoolExecutor

from conftest import create_member, create_workshop, outbox_rows
from workshop_service.registration import RegistrationError, cancel, register


def test_registration_saves_confirmation_intent(client, db):
    member = create_member(client, "Asha")
    workshop = create_workshop(client, startsAt="2026-11-20T10:00:00Z")

    response = client.post(f"/workshops/{workshop['id']}/register", json={"memberId": member["id"]})
    assert response.status_code == 201
    body = response.get_json()
    assert (body["status"], body["confirmation"]) == ("registered", "pending")

    [row] = outbox_rows(db)
    assert (row["kind"], row["status"], row["attempts"]) == ("confirmation", "pending", 0)
    assert row["idempotency_key"] == f"confirmation:{body['registrationId']}"
    assert row["payload"]["email"] == "asha@campus.edu"
    assert row["payload"]["startsAt"] == "2026-11-20T10:00:00+00:00"


def test_duplicate_registration_is_rejected_without_notification(client, db):
    member = create_member(client, "Asha")
    workshop = create_workshop(client)
    client.post(f"/workshops/{workshop['id']}/register", json={"memberId": member["id"]})

    again = client.post(f"/workshops/{workshop['id']}/register", json={"memberId": member["id"]})
    assert again.status_code == 409
    assert len(outbox_rows(db)) == 1


def test_registration_errors(client, db):
    member = create_member(client, "Asha")
    workshop = create_workshop(client, capacity=1)
    assert client.post("/workshops/99/register", json={"memberId": member["id"]}).status_code == 404
    assert client.post(f"/workshops/{workshop['id']}/register", json={"memberId": 99}).status_code == 404
    assert client.post(f"/workshops/{workshop['id']}/register", json={"memberId": "1"}).status_code == 400

    client.post(f"/workshops/{workshop['id']}/register", json={"memberId": member["id"]})
    other = create_member(client, "Ben")
    full = client.post(f"/workshops/{workshop['id']}/register", json={"memberId": other["id"]})
    assert (full.status_code, full.get_json()["error"]) == (409, "workshop is full")

    client.post(f"/workshops/{workshop['id']}/cancel")
    cancelled = create_workshop(client)
    client.post(f"/workshops/{cancelled['id']}/cancel")
    late = client.post(f"/workshops/{cancelled['id']}/register", json={"memberId": other["id"]})
    assert (late.status_code, late.get_json()["error"]) == (409, "workshop is cancelled")
    assert [r["kind"] for r in outbox_rows(db)] == ["confirmation", "cancellation"]


def test_validation(client):
    assert client.post("/members", json={"name": "A", "email": "no-at-sign"}).status_code == 400
    create_member(client, "Asha")
    assert client.post("/members", json={"name": "A", "email": "ASHA@campus.edu"}).status_code == 409
    assert client.post("/workshops", json={"title": ""}).status_code == 400
    assert client.post("/workshops", json={"title": "x", "capacity": 0}).status_code == 400
    assert client.post("/workshops", json={"title": "x", "startsAt": "soon"}).status_code == 400


def test_concurrent_duplicate_registrations_produce_one_registration(client, db):
    member = create_member(client, "Asha")
    workshop = create_workshop(client)

    def attempt(_):
        try:
            register(db, workshop["id"], member["id"])
            return 201
        except RegistrationError as exc:
            return exc.status

    with ThreadPoolExecutor(max_workers=10) as pool:
        statuses = list(pool.map(attempt, range(20)))
    assert statuses.count(201) == 1
    assert statuses.count(409) == 19
    assert len(outbox_rows(db)) == 1


def test_capacity_holds_under_concurrency(client, db):
    workshop = create_workshop(client, capacity=5)
    members = [create_member(client, f"m{i}") for i in range(20)]

    def attempt(member):
        try:
            register(db, workshop["id"], member["id"])
            return True
        except RegistrationError:
            return False

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(attempt, members))
    assert results.count(True) == 5
    assert client.get(f"/workshops/{workshop['id']}").get_json()["registered"] == 5
    assert len(outbox_rows(db)) == 5


def test_cancel_notifies_every_registration_even_when_racing_registrations(client, db):
    workshop = create_workshop(client)
    members = [create_member(client, f"m{i}") for i in range(30)]
    start = threading.Barrier(31)

    def attempt(member):
        start.wait()
        try:
            register(db, workshop["id"], member["id"])
        except RegistrationError:
            pass

    def cancel_midway():
        start.wait()
        cancel(db, workshop["id"])

    threads = [threading.Thread(target=attempt, args=(m,)) for m in members] + [threading.Thread(target=cancel_midway)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    registered = db.fetch_one("SELECT count(*) AS n FROM registrations")["n"]
    rows = outbox_rows(db)
    assert sum(r["kind"] == "confirmation" for r in rows) == registered
    assert sum(r["kind"] == "cancellation" for r in rows) == registered


def test_cancel_is_atomic_ordered_and_idempotent(client, db):
    workshop = create_workshop(client)
    for name in ("Asha", "Ben", "Chen"):
        client.post(f"/workshops/{workshop['id']}/register", json={"memberId": create_member(client, name)["id"]})

    first = client.post(f"/workshops/{workshop['id']}/cancel").get_json()
    assert (first["workshop"]["status"], first["notificationsQueued"], first["alreadyCancelled"]) == (
        "cancelled", 3, False)
    second = client.post(f"/workshops/{workshop['id']}/cancel").get_json()
    assert (second["notificationsQueued"], second["alreadyCancelled"]) == (0, True)
    assert client.post("/workshops/99/cancel").status_code == 404

    rows = outbox_rows(db, workshop["id"])
    confirmations = {r["registration_id"]: r["id"] for r in rows if r["kind"] == "confirmation"}
    cancellations = [r for r in rows if r["kind"] == "cancellation"]
    assert len(cancellations) == 3
    for row in cancellations:
        assert row["depends_on"] == confirmations[row["registration_id"]]
        assert row["payload"]["event"] == "workshop_cancelled"

    registrations = client.get(f"/workshops/{workshop['id']}/registrations").get_json()["items"]
    assert {(r["confirmationStatus"], r["cancellationStatus"]) for r in registrations} == {("pending", "pending")}
