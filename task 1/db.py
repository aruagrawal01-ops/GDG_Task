import os
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


@contextmanager
def get_cursor():
    conn = psycopg2.connect(DATABASE_URL, options="-c timezone=UTC")
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                yield cur
    finally:
        conn.close()


def create_tables():
    with open(os.path.join(os.path.dirname(__file__), "schema.sql")) as f:
        schema = f.read()
    with get_cursor() as cur:
        cur.execute(schema)


if __name__ == "__main__":
    create_tables()
    print("Tables are ready.")
