import logging
import os

from dotenv import load_dotenv

from .notifier import create_app


def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [notifier] %(message)s")
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    port = int(os.getenv("NOTIFIER_PORT", "8002"))
    create_app().run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
