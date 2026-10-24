import os
import random
import threading
import time

from flask import Flask, jsonify, request

FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.2"))
SLOW_RATE = float(os.getenv("SLOW_RATE", "0.1"))
SLOW_SECONDS = 5

app = Flask(__name__)
delivered = {}
lock = threading.Lock()


@app.post("/notify")
def notify():
    key = request.headers.get("Idempotency-Key")
    message = request.get_json(silent=True)
    if not key or not isinstance(message, dict):
        return jsonify(error="an Idempotency-Key header and a JSON body are required"), 400

    roll = random.random()
    if roll < FAILURE_RATE:
        print(f"ERROR      {key}: simulated failure")
        return jsonify(error="simulated failure"), 500

    with lock:
        duplicate = key in delivered
        delivered.setdefault(key, message)
    if duplicate:
        print(f"DUPLICATE  {key}: already delivered, ignored")
    else:
        print(f"DELIVERED  {key}: {message.get('kind')} for {message.get('member_id')}")

    if roll < FAILURE_RATE + SLOW_RATE:
        print(f"SLOW       {key}: replying after {SLOW_SECONDS}s, the worker will time out")
        time.sleep(SLOW_SECONDS)
    return jsonify(status="duplicate" if duplicate else "delivered")


@app.get("/deliveries")
def deliveries():
    with lock:
        return jsonify(delivered)


if __name__ == "__main__":
    app.run(port=8002, threaded=True)
