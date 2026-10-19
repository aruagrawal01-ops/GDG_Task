import logging
import signal

from workshop_service.config import WorkerConfig as Config
from workshop_service.db import Database

from .worker import OutboxWorker, http_sender


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [worker] %(message)s")
    db = Database(Config.DATABASE_URL, Config.CONCURRENCY + 2)
    db.init_schema()
    worker = OutboxWorker(
        db,
        http_sender(Config.NOTIFIER_URL, Config.CONNECT_TIMEOUT, Config.READ_TIMEOUT),
        concurrency=Config.CONCURRENCY,
        lease_seconds=Config.LEASE_SECONDS,
        max_attempts=Config.MAX_ATTEMPTS,
        backoff_base=Config.BACKOFF_BASE_SECONDS,
        backoff_cap=Config.BACKOFF_CAP_SECONDS,
        poll_interval=Config.POLL_INTERVAL,
    )
    signal.signal(signal.SIGTERM, lambda *_: worker.stop())
    signal.signal(signal.SIGINT, lambda *_: worker.stop())
    worker.run()
    db.close()


if __name__ == "__main__":
    main()
