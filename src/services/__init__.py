import os
from .service_registry import ServiceRegistry, Config, DatabaseParams


config = Config(
    database_params=DatabaseParams(
        os.getenv("DB_USER"),
        os.getenv("DB_PASSWORD"),
        os.getenv("DB_HOST"),
        os.getenv("DB_PORT"),
        os.getenv("DB_NAME"),
    ),
    alpaca_key=os.getenv("ALPACA_KEY"),
    alpaca_secret=os.getenv("ALPACA_SECRET"),
    kafka_broker="TEST"
)

services = ServiceRegistry(config=config)
