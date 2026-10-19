import threading
from contextlib import contextmanager
from pathlib import Path

from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

SCHEMA_FILE = Path(__file__).with_name("schema.sql")


class Database:
    def __init__(self, dsn, max_connections):
        self.pool = ThreadedConnectionPool(1, max_connections, dsn, options="-c timezone=UTC")
        self._slots = threading.BoundedSemaphore(max_connections)

    @contextmanager
    def connection(self, wait_seconds=10):
        if not self._slots.acquire(timeout=wait_seconds):
            raise OperationalError("no database connection available")
        try:
            conn = self.pool.getconn()
            try:
                yield conn
                conn.commit()
            except Exception:
                if not conn.closed:
                    conn.rollback()
                raise
            finally:
                self.pool.putconn(conn, close=bool(conn.closed))
        finally:
            self._slots.release()

    def fetch_all(self, sql, params=()):
        with self.connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def fetch_one(self, sql, params=()):
        with self.connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def init_schema(self):
        with self.connection() as conn, conn.cursor() as cur:
            cur.execute(SCHEMA_FILE.read_text())

    def close(self):
        if not self.pool.closed:
            self.pool.closeall()
