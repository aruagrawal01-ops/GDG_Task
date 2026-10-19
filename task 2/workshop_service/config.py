import os

from dotenv import load_dotenv

load_dotenv()


def _int(name, default):
    return int(os.getenv(name, default))


def _float(name, default):
    return float(os.getenv(name, default))


class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres@localhost:5432/workshops")
    DB_POOL_MAX = _int("DB_POOL_MAX", 20)


class WorkerConfig:
    DATABASE_URL = Config.DATABASE_URL
    NOTIFIER_URL = os.getenv("NOTIFIER_URL", "http://localhost:8002/notify")
    CONCURRENCY = _int("WORKER_CONCURRENCY", 8)
    LEASE_SECONDS = _int("LEASE_SECONDS", 30)
    MAX_ATTEMPTS = _int("MAX_ATTEMPTS", 50)
    BACKOFF_BASE_SECONDS = _float("BACKOFF_BASE_SECONDS", 2)
    BACKOFF_CAP_SECONDS = _float("BACKOFF_CAP_SECONDS", 300)
    CONNECT_TIMEOUT = _float("NOTIFY_CONNECT_TIMEOUT", 1)
    READ_TIMEOUT = _float("NOTIFY_READ_TIMEOUT", 2)
    POLL_INTERVAL = _float("POLL_INTERVAL", 0.5)
