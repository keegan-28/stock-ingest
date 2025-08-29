from typing import Any, Type
from sqlalchemy import create_engine, MetaData, Table, Column, inspect, text, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import String, Float, TIMESTAMP, Integer
from pydantic import BaseModel
from datetime import datetime
from src.utils.logger import logger


class PostgresDB:
    def __init__(self, user: str, password: str, host: str, port: int, db: str) -> None:
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
            isolation_level="READ COMMITTED",  # ensures immediate visibility
        )
        self.metadata = MetaData()

    def table_exists(self, table_name: str) -> bool:
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def create_table(
        self,
        table_name: str,
        table_model: Type[BaseModel],
    ) -> None:
        if self.table_exists(table_name):
            logger.info(f"{table_name} already exists.")
            return

        columns = []
        primary_keys = []
        for name, field_info in table_model.model_fields.items():
            if field_info.json_schema_extra and "primary_key" in field_info.json_schema_extra:
                primary_keys.append(name)

            col_type = self._map_pydantic_type(field_info.annotation)
            pk = primary_keys is not None and name in primary_keys
            columns.append(Column(name, col_type, primary_key=pk))

        table = Table(table_name, self.metadata, *columns)
        self.metadata.create_all(self.engine, tables=[table])

    def insert_items(
        self,
        table_name: str,
        items: list[BaseModel],
    ) -> None:
        """Insert items and commit immediately"""
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        values = [item.model_dump() for item in items]

        stmt = insert(table).values(values)
        primary_keys = [c.name for c in table.primary_key]
        stmt = stmt.on_conflict_do_nothing(index_elements=primary_keys)

        # Use a single transaction that commits automatically
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def fetch_items(self, query: str, params: dict | None = None) -> list[Any]:
        """Fetch rows using a simple connection (no extra transaction)"""
        clause = text(query)
        with self.engine.connect() as conn:
            return conn.execute(clause, parameters=params).fetchall()

    def list_tables(self) -> list[str]:
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def delete_ticker(self, table_name: str, column_name: str, value: str) -> None:
        """Delete all rows where column_name = value"""
        table = Table(table_name, self.metadata, autoload_with=self.engine)

        if not hasattr(table.c, column_name):
            raise ValueError(f"Column '{column_name}' does not exist in table '{table_name}'")

        stmt = delete(table).where(getattr(table.c, column_name) == value)

        with self.engine.begin() as conn:
            result = conn.execute(stmt)
            logger.info(
                f"Deleted {result.rowcount} rows from '{table_name}' where {column_name}={value}"
            )

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
