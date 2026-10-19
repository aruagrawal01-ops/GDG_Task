import os

from dotenv import load_dotenv

load_dotenv()


def _int(name, default):
    return int(os.getenv(name, default))


class Config:
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres@localhost:5432/sensors")
    DB_POOL_MAX = _int("DB_POOL_MAX", 24)
    QUEUE_CAPACITY = _int("QUEUE_CAPACITY", 50000)
    FLUSH_MAX_READINGS = _int("FLUSH_MAX_READINGS", 5000)
    FLUSH_WAIT_MS = _int("FLUSH_WAIT_MS", 100)
    MAX_BATCH_SIZE = _int("MAX_BATCH_SIZE", 5000)
    RULE_REFRESH_SECONDS = _int("RULE_REFRESH_SECONDS", 5)
    START_CONSUMER = True
