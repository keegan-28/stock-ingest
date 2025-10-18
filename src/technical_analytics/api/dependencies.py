from src.technical_analytics.services import services
from src.technical_analytics.services.broker import AlpacaBroker
from src.technical_analytics.services.database import PostgresDB


def get_db() -> PostgresDB:
    return services.db


def get_broker() -> AlpacaBroker:
    return services.broker
