import atexit
import logging

from flask import Flask, jsonify
from psycopg2 import OperationalError
from werkzeug.exceptions import HTTPException

from .config import Config
from .db import Database
from .engine import RuleEngine
from .ingest import Ingestor
from .routes import alerts, readings, rules, system


def create_app(overrides=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    app = Flask(__name__)
    app.config.from_object(Config)
    if overrides:
        app.config.update(overrides)

    db = Database(app.config["DATABASE_URL"], app.config["DB_POOL_MAX"])
    db.init_schema()
    ingestor = Ingestor(
        db,
        RuleEngine(),
        queue_capacity=app.config["QUEUE_CAPACITY"],
        flush_max=app.config["FLUSH_MAX_READINGS"],
        flush_wait_ms=app.config["FLUSH_WAIT_MS"],
        rule_refresh_seconds=app.config["RULE_REFRESH_SECONDS"],
    )
    app.extensions["db"] = db
    app.extensions["ingestor"] = ingestor

    for module in (readings, rules, alerts, system):
        app.register_blueprint(module.bp)

    @app.errorhandler(HTTPException)
    def http_error(exc):
        return jsonify(error=exc.description), exc.code

    @app.errorhandler(OperationalError)
    def database_unavailable(exc):
        app.logger.error("database error: %s", exc)
        return jsonify(error="database unavailable"), 503

    if app.config["START_CONSUMER"]:
        ingestor.start()
        atexit.register(ingestor.stop)
    return app
