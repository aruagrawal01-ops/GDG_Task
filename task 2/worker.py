import os
import time

import requests
from psycopg2 import OperationalError

from db import get_cursor

NOTIFIER_URL = os.getenv("NOTIFIER_URL", "http://localhost:8002/notify")
TIMEOUT_SECONDS = 2
MAX_RETRY_DELAY_SECONDS = 60

NEXT_DUE_NOTIFICATION = """
SELECT n.id, n.kind, n.attempts, r.member_id, w.id AS workshop_id, w.title
FROM notifications n
JOIN registrations r ON r.id = n.registration_id
JOIN workshops w ON w.id = r.workshop_id
WHERE n.status = 'pending' AND n.next_attempt_at <= now()
ORDER BY n.next_attempt_at
LIMIT 1
FOR UPDATE OF n SKIP LOCKED
"""


def send(notification):
    message = {
        "kind": notification["kind"],
        "member_id": notification["member_id"],
        "workshop_id": notification["workshop_id"],
        "workshop_title": notification["title"],
    }
    headers = {"Idempotency-Key": f"notification-{notification['id']}"}
    try:
        response = requests.post(NOTIFIER_URL, json=message, headers=headers, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        return type(exc).__name__
    if 200 <= response.status_code < 300:
        return None
    return f"HTTP {response.status_code}"


def process_next():
    with get_cursor() as cur:
        cur.execute(NEXT_DUE_NOTIFICATION)
        notification = cur.fetchone()
        if notification is None:
            return False

        attempt = notification["attempts"] + 1
        label = f"{notification['kind']} for {notification['member_id']} (attempt {attempt})"
        problem = send(notification)
        if problem is None:
            cur.execute(
                "UPDATE notifications SET status = 'sent', attempts = %s, last_error = NULL WHERE id = %s",
                (attempt, notification["id"]),
            )
            print(f"SENT    {label}")
        else:
            delay = min(2 ** attempt, MAX_RETRY_DELAY_SECONDS)
            cur.execute(
                """
                UPDATE notifications
                SET attempts = %s, last_error = %s, next_attempt_at = now() + make_interval(secs => %s)
                WHERE id = %s
                """,
                (attempt, problem, delay, notification["id"]),
            )
            print(f"FAILED  {label}: {problem}, retrying in {delay}s")
    return True


def main():
    print(f"Worker started, sending to {NOTIFIER_URL}")
    while True:
        try:
            if not process_next():
                time.sleep(1)
        except OperationalError as exc:
            print(f"Database unavailable ({exc}), retrying in 5s")
            time.sleep(5)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Worker stopped.")
