from pydantic import BaseModel
from typing import TypedDict
from src.services.database import PostgresDB
from src.services.broker import AlpacaBroker


class DatabaseParams(BaseModel):
    user: str
    password: str
    host: str
    port: int
    database_name: str


class Config(BaseModel):
    database_params: DatabaseParams
    alpaca_key: str
    alpaca_secret: str
    kafka_broker: list[str]


class ServicesDict(TypedDict, total=False):
    db: PostgresDB
    broker: AlpacaBroker


class ServiceRegistry:
    def __init__(self, config: Config) -> None:
        self.__config = config
        self._services: ServicesDict = {}

    def get_db_conn(self) -> PostgresDB:
        if "db" not in self._services:
            self._services["db"] = PostgresDB(
                self.__config.database_params.user,
                self.__config.database_params.password,
                self.__config.database_params.host,
                self.__config.database_params.port,
                self.__config.database_params.database_name,
            )
        return self._services["db"]

    def get_broker_conn(self) -> AlpacaBroker:
        if "broker" not in self._services:
            self._services["broker"] = AlpacaBroker(
                self.__config.alpaca_key, self.__config.alpaca_secret
            )
        return self._services["broker"]

    def get_kafka_conn(self) -> None:
        pass
