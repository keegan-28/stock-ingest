from pydantic import BaseModel
from src.services.database import PostgresDB
from src.services.broker import AlpacaBroker
from src.utils.logger import logger
import threading


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


class ServiceRegistry:
    def __init__(self, config: Config) -> None:
        self.__config = config
        self._db: PostgresDB | None = None
        self._broker: AlpacaBroker | None = None
        self._kafka: None = None  # placeholder
        self._lock = threading.Lock()

    @property
    def db(self) -> PostgresDB:
        if self._db is None:
            with self._lock:
                logger.info("Initialising Database Connection")
                self._db = PostgresDB(
                    self.__config.database_params.user,
                    self.__config.database_params.password,
                    self.__config.database_params.host,
                    self.__config.database_params.port,
                    self.__config.database_params.database_name,
                )
        return self._db

    @property
    def broker(self) -> AlpacaBroker:
        if self._broker is None:
            logger.info("Initialising Broker Connection")

            self._broker = AlpacaBroker(self.__config.alpaca_key, self.__config.alpaca_secret)
        return self._broker

    @property
    def kafka(self):
        if self._kafka is None:
            logger.info("Initialising Kafka Connection")
            pass
        return self._kafka
