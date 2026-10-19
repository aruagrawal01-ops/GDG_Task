import os

bind = os.getenv("BIND", "0.0.0.0:8000")
workers = 1
worker_class = "gthread"
threads = int(os.getenv("GUNICORN_THREADS", "16"))
backlog = 2048
keepalive = 5
timeout = 30
graceful_timeout = 30
