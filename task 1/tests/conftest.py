import os
import time

import pytest

from sensor_service import create_app

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "postgresql://postgres@localhost:5432/sensors_test")


def make_app(**overrides):
    return create_app({"DATABASE_URL": TEST_DATABASE_URL, "FLUSH_WAIT_MS": 10, "TESTING": True, **overrides})


def shutdown(app):
    app.extensions["ingestor"].stop()
    app.extensions["db"].close()


def reset_database():
    app = make_app(START_CONSUMER=False)
    with app.extensions["db"].connection() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE readings, alerts, alert_rules RESTART IDENTITY CASCADE")
    shutdown(app)


def wait_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    raise AssertionError("condition not met in time")


def wait_for_flush(app):
    ingestor = app.extensions["ingestor"]

    def drained():
        counters = ingestor.stats.snapshot()
        return (ingestor.queue.depth == 0 and ingestor.in_flight == 0
                and counters["written_readings"] >= counters["accepted_readings"])

    wait_until(drained)


@pytest.fixture
def app():
    reset_database()
    app = make_app()
    yield app
    shutdown(app)


@pytest.fixture
def client(app):
    return app.test_client()


def reading(value, sensor="library-floor-2", reading_type="temperature", timestamp="2026-10-12T10:30:00Z"):
    return {"sensorId": sensor, "type": reading_type, "value": value, "timestamp": timestamp}
