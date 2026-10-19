"""End-to-end demonstration: registration, duplicates, notification failure, recovery,
a backend crash (kill -9) and workshop cancellation.

It starts the mock notifier, the API and the outbox worker itself, so stop any running
copies first. Process logs are written to demo-logs/.

  python simulator/demo.py
"""
import os
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "demo-logs"
API = "http://localhost:8001"
NOTIFIER = "http://localhost:8002"
PY = sys.executable

load_dotenv(ROOT / ".env")
ENV = {
    **os.environ,
    "BACKOFF_BASE_SECONDS": "0.5",
    "BACKOFF_CAP_SECONDS": "3",
    "LEASE_SECONDS": "10",
    "NOTIFIER_FAILURE_RATE": "0",
    "NOTIFIER_TIMEOUT_RATE": "0",
    "NOTIFIER_TIMEOUT_SECONDS": "3",
    "NOTIFIER_DB": str(LOGS / "notifier.sqlite3"),
    "PYTHONUNBUFFERED": "1",
}
COMMANDS = {
    "notifier": [PY, "-m", "mock_notifier"],
    "api": [PY, "-m", "gunicorn", "-c", "gunicorn.conf.py", "wsgi:app"],
    "worker": [PY, "-m", "outbox_worker"],
}
processes = {}


def start(name):
    log = open(LOGS / f"{name}.log", "a")
    processes[name] = subprocess.Popen(COMMANDS[name], cwd=ROOT, env=ENV, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)


def stop(name, sig=signal.SIGTERM):
    process = processes.pop(name, None)
    if process and process.poll() is None:
        os.killpg(process.pid, sig)
        process.wait(timeout=30)


def step(title):
    print(f"\n=== {title} " + "=" * max(0, 70 - len(title)))


def wait_for(check, timeout=60, interval=0.25):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except requests.RequestException:
            pass
        time.sleep(interval)
    raise SystemExit("timed out waiting for the expected state, see demo-logs/")


def up(url):
    return lambda: requests.get(url, timeout=1).ok


def post(path, body=None):
    return requests.post(f"{API}{path}", json=body or {}, timeout=5)


def outbox(workshop_id):
    return requests.get(f"{API}/outbox", params={"workshopId": workshop_id}, timeout=5).json()["items"]


def all_sent(workshop_id, expected):
    rows = outbox(workshop_id)
    return len(rows) == expected and all(r["status"] == "sent" for r in rows)


def show_outbox(workshop_id):
    print(f"  {'id':>4}  {'idempotency key':<18} {'status':<8} {'attempts':>8}  last error")
    for row in outbox(workshop_id):
        print(f"  {row['id']:>4}  {row['idempotencyKey']:<18} {row['status']:<8} {row['attempts']:>8}  "
              f"{row['lastError'] or ''}")


def register(workshop_id, member, label=""):
    started = time.perf_counter()
    response = post(f"/workshops/{workshop_id}/register", {"memberId": member["id"]})
    elapsed = (time.perf_counter() - started) * 1000
    print(f"  register {member['name']:<6} -> HTTP {response.status_code} in {elapsed:.0f} ms  "
          f"{response.json()} {label}")
    return response


def main():
    if any(_responds(url) for url in (f"{API}/health", f"{NOTIFIER}/stats")):
        raise SystemExit("ports 8001/8002 are in use: stop the running API and notifier first")
    LOGS.mkdir(exist_ok=True)
    (LOGS / "notifier.sqlite3").unlink(missing_ok=True)
    try:
        run_demo()
    finally:
        for name in list(processes):
            stop(name)


def _responds(url):
    try:
        requests.get(url, timeout=0.5)
        return True
    except requests.RequestException:
        return False


def run_demo():
    step("1. Start the notifier, the API and the outbox worker")
    for name in ("notifier", "api", "worker"):
        start(name)
    wait_for(up(f"{API}/health"))
    wait_for(up(f"{NOTIFIER}/stats"))
    print(f"  GET /health -> {requests.get(f'{API}/health').json()}")

    tag = uuid.uuid4().hex[:6]
    members = [post("/members", {"name": name, "email": f"{name.lower()}.{tag}@campus.edu"}).json()
               for name in ("Asha", "Ben", "Chen", "Dev", "Eli", "Fatima", "Gita")]
    workshop = post("/workshops", {"title": f"Backend Basics {tag}", "startsAt": "2026-11-20T10:00:00Z"}).json()
    wid = workshop["id"]
    print(f"  created workshop {wid} and members {[m['id'] for m in members]}")

    step("2. Registration returns at once; confirmations are sent in the background")
    for member in members[:3]:
        register(wid, member)
    wait_for(lambda: all_sent(wid, 3))
    show_outbox(wid)

    step("3. Duplicate registration is rejected and creates no notification")
    register(wid, members[0], "(expected 409)")
    print(f"  outbox rows for this workshop: {len(outbox(wid))} (unchanged)")

    step("4. Lost acknowledgement: the notifier delivers but replies after the worker's timeout")
    requests.post(f"{NOTIFIER}/admin/config", json={"timeoutNext": 1})
    register(wid, members[3])
    wait_for(lambda: all_sent(wid, 4))
    show_outbox(wid)
    delivery = requests.get(f"{NOTIFIER}/deliveries", params={"memberId": members[3]["id"]}).json()["items"][0]
    print(f"  notifier: {delivery['idempotencyKey']} delivered once, duplicates ignored: {delivery['duplicates']}")

    step("5. Notification failure: the notifier is down")
    stop("notifier")
    register(wid, members[4])
    register(wid, members[5])
    time.sleep(4)
    print("  rows stay pending, attempts grow with backoff:")
    show_outbox(wid)

    step("6. Recovery: start the notifier, pending rows drain")
    start("notifier")
    wait_for(lambda: all_sent(wid, 6))
    show_outbox(wid)

    step("7. Backend crash: kill -9 the API and the worker right after a registration")
    stop("notifier")
    register(wid, members[6])
    stop("api", signal.SIGKILL)
    stop("worker", signal.SIGKILL)
    with psycopg2.connect(ENV["DATABASE_URL"]) as conn, conn.cursor() as cur:
        cur.execute("SELECT idempotency_key, status, attempts FROM outbox "
                    "WHERE workshop_id = %s AND status = 'pending'", (wid,))
        print(f"  API and worker killed. Pending rows still in PostgreSQL: {cur.fetchall()}")
    conn.close()
    for name in ("api", "worker", "notifier"):
        start(name)
    wait_for(up(f"{API}/health"))
    wait_for(lambda: all_sent(wid, 7))
    print("  after restart the confirmation was delivered:")
    show_outbox(wid)

    step("8. Cancellation with a flaky notifier (50% errors)")
    requests.post(f"{NOTIFIER}/admin/config", json={"failureRate": 0.5})
    result = post(f"/workshops/{wid}/cancel").json()
    print(f"  cancel -> status {result['workshop']['status']}, notifications queued: {result['notificationsQueued']}")
    print(f"  cancel again -> {post(f'/workshops/{wid}/cancel').json()['notificationsQueued']} queued (idempotent)")
    wait_for(lambda: all_sent(wid, 14), timeout=120)
    show_outbox(wid)

    step("9. What each member received (from the notifier)")
    for item in requests.get(f"{NOTIFIER}/deliveries", params={"workshopId": wid}).json()["items"]:
        print(f"  {item['email']:<28} {item['event']:<24} duplicates ignored: {item['duplicates']}")
    print(f"\n  notifier counters since its last start: {requests.get(f'{NOTIFIER}/stats').json()['counters']}")
    print("\nDemo complete. Process logs are in demo-logs/.")


if __name__ == "__main__":
    main()
