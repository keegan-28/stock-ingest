from typing import Any, Type
from sqlalchemy import create_engine, MetaData, Table, Column, inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import String, Float, TIMESTAMP, Integer
from pydantic import BaseModel
from datetime import datetime
from src.utils.logger import logger


class PostgresDB:
    def __init__(self, user: str, password: str, host: str, port: int, db: str) -> None:
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
        self.metadata = MetaData()

    def table_exists(self, table_name: str) -> bool:
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def create_table(
        self,
        table_name: str,
        table_model: Type[BaseModel],
        primary_keys: list[str] | None = None,
    ) -> None:
        if self.table_exists(table_name):
            logger.info(f"{table_name} already exists.")
            return

        columns = []
        for name, field in table_model.model_fields.items():
            col_type = self._map_pydantic_type(field.annotation)
            pk = primary_keys is not None and name in primary_keys
            columns.append(Column(name, col_type, primary_key=pk))

        table = Table(table_name, self.metadata, *columns)
        self.metadata.create_all(self.engine, tables=[table])

    def insert_items(
        self,
        table_name: str,
        items: list[BaseModel],
        conflict_cols: list[str] | None = None,
    ) -> None:
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        values = [item.model_dump() for item in items]

        stmt = insert(table).values(values)
        if conflict_cols:
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

        with self.engine.begin() as conn:
            conn.execute(stmt)

    def fetch_items(self, table_name: str, query: str, params: dict | None = None) -> list[Any]:
        clause = text(query)

        with self.engine.begin() as conn:
            ticks: list[Any] = conn.execute(clause, parameters=params).fetchall()
        return ticks

    def list_tables(self) -> list[str]:
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def _map_pydantic_type(self, py_type: Any) -> Any:
        if py_type is str:
            return String
        elif py_type is float:
            return Float
        elif py_type is int:
            return Integer
        elif py_type is datetime:
            return TIMESTAMP
        else:
            return String
