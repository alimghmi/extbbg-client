import importlib
import json
import logging
import os
import sys

from decouple import config

from config import get_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)s]: %(message)s",
)

APP = config("APP", cast=str)
BBG_CRED = json.loads(config("BBG_CRED", cast=str))
logger = logging.getLogger(__name__)


def app_exist(app):
    path = os.path.dirname(__file__)
    return app in (
        entry for entry in os.listdir(path) if os.path.isdir(os.path.join(path, entry))
    )


def load_app(app):
    app_config = get_config(app)
    if not app_config:
        raise ValueError(f"{app} app config not found")

    if not app_exist(app_config["app_name"]):
        raise ValueError(f"{app} app not found")

    path = os.path.dirname(__file__)
    sys.path.append(os.path.join(path, app_config["app_name"]))
    loader_instance = getattr(importlib.import_module("loader"), "Tickers")(
        app_config["input"]["table"],
        app_config["input"]["columns"],
        app_config["input"]["where"],
    )
    client_class = getattr(importlib.import_module("client"), "Client")
    return loader_instance, client_class, app_config


def main():
    loader, Client, app_config = load_app(APP)
    logger.info(f"Launching {APP} App...")
    tickers = loader.fetch()
    client = Client(BBG_CRED, app_config)
    universe = client.create_universe(tickers)
    field = app_config["field_url"]
    trigger = client.get_trigger()
    client.request(universe, field, trigger)
    client.listen()
    client.save()


if __name__ == "__main__":
    main()
