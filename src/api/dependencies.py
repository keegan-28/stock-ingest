from src.services import services
from src.services.broker import AlpacaBroker
from src.services.database import PostgresDB


def get_db() -> PostgresDB:
    return services.db


def get_broker() -> AlpacaBroker:
    return services.broker
