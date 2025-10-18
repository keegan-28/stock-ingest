import os
from .service_registry import ServiceRegistry, Config, DatabaseParams


config = Config(
    database_params=DatabaseParams(
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        database_name=os.environ["DB_NAME"],
    ),
    alpaca_key=os.environ["ALPACA_KEY"],
    alpaca_secret=os.environ["ALPACA_SECRET"],
    kafka_broker=["TEST"],
)

services = ServiceRegistry(config=config)
