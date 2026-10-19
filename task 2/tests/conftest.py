import os

import pytest

from outbox_worker.worker import OutboxWorker
from workshop_service import create_app

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "postgresql://postgres@localhost:5432/workshops_test")


@pytest.fixture
def app():
    app = create_app({"DATABASE_URL": TEST_DATABASE_URL, "TESTING": True})
    with app.extensions["db"].connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE outbox, registrations, members, workshops RESTART IDENTITY CASCADE")
    yield app
    app.extensions["db"].close()


@pytest.fixture
def db(app):
    return app.extensions["db"]


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def make_worker(db):
    def factory(send, **options):
        settings = {"concurrency": 4, "lease_seconds": 30, "max_attempts": 5,
                    "backoff_base": 0.0, "backoff_cap": 0.0, **options}
        return OutboxWorker(db, send, **settings)

    return factory


def create_member(client, name):
    return client.post("/members", json={"name": name, "email": f"{name.lower()}@campus.edu"}).get_json()


def create_workshop(client, **fields):
    return client.post("/workshops", json={"title": "Backend Basics", **fields}).get_json()


def outbox_rows(db, workshop_id=None):
    if workshop_id is None:
        return db.fetch_all("SELECT * FROM outbox ORDER BY id")
    return db.fetch_all("SELECT * FROM outbox WHERE workshop_id = %s ORDER BY id", (workshop_id,))
