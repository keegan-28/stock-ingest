from typing import Any, Type
from sqlmodel import SQLModel
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, Table, inspect, text, delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.types import String, Float, TIMESTAMP, Integer, JSON
from pydantic import BaseModel
from datetime import datetime
from src.utils.logger import logger


class PostgresDB:
    def __init__(self, user: str, password: str, host: str, port: int, db: str) -> None:
        self.engine = create_engine(
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
            isolation_level="READ COMMITTED",  # ensures immediate visibility
            echo=False,
        )

    def table_exists(self, table_name: str) -> bool:
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def create_table(self, table_model: Type[SQLModel]) -> None:
        """Create table for a SQLModel if it doesn't exist"""
        table_name = table_model.__tablename__
        if self.table_exists(table_name):
            logger.info(f"{table_name} already exists.")
            return

        SQLModel.metadata.create_all(self.engine, tables=[table_model.__table__])
        logger.info(f"Created table {table_name}")

    def insert_items(self, items: list[SQLModel]) -> None:
        """Insert SQLModel items with upsert-like safety (ignore conflicts)"""
        if not items:
            return

        model_cls = type(items[0])
        table = model_cls.__table__
        values = [item.model_dump() for item in items]

        stmt = insert(table).values(values).on_conflict_do_nothing()

        try:
            with self.engine.begin() as conn:
                conn.execute(stmt)
                conn.commit()
        except SQLAlchemyError as e:
            # Log unexpected DB errors
            logger.exception(f"Failed to insert items into {table.name}: {e}")
            raise

    def fetch_items(self, query: str, params: dict | None = None) -> list[Any]:
        """Run raw SQL query and return results"""
        with self.engine.connect() as conn:
            result = conn.execute(text(query), parameters=params)
            return result.fetchall()

    def list_tables(self) -> list[str]:
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def delete_ticker(self, model: Type[SQLModel], column_name: str, value: str) -> None:
        if not hasattr(model, column_name):
            raise ValueError(f"Column '{column_name}' does not exist in model '{model.__name__}'")

        stmt = delete(model).where(getattr(model, column_name) == value)

        with self.engine.begin() as conn:
            result = conn.execute(stmt)
            conn.commit()

        logger.info(
            f"Deleted {result.rowcount} rows from '{model.__tablename__}' "
            f"where {column_name}={value}"
        )
