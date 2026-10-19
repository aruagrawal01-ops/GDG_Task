import logging

from flask import Flask, jsonify
from psycopg2 import OperationalError
from werkzeug.exceptions import HTTPException

from .config import Config
from .db import Database
from .routes import members, outbox, system, workshops


def create_app(overrides=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    app = Flask(__name__)
    app.config.from_object(Config)
    if overrides:
        app.config.update(overrides)

    db = Database(app.config["DATABASE_URL"], app.config["DB_POOL_MAX"])
    db.init_schema()
    app.extensions["db"] = db

    for module in (members, workshops, outbox, system):
        app.register_blueprint(module.bp)

    @app.errorhandler(HTTPException)
    def http_error(exc):
        return jsonify(error=exc.description), exc.code

    @app.errorhandler(OperationalError)
    def database_unavailable(exc):
        app.logger.error("database error: %s", exc)
        return jsonify(error="database unavailable"), 503

    return app
