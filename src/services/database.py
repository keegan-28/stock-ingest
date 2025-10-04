from typing import Any, Sequence
from sqlalchemy.engine import Row
from sqlmodel import SQLModel
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, inspect, text, delete
from sqlalchemy.dialects.postgresql import insert
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

    def create_table(self, table_model: type[SQLModel]) -> None:
        """Create table for a SQLModel if it doesn't exist"""
        table_name: str = table_model.__tablename__
        if self.table_exists(table_name):
            logger.info(f"{table_name} already exists.")
            return

        SQLModel.metadata.create_all(self.engine, tables=[table_model.__table__])
        logger.info(f"Created table {table_name}")

    def insert_items(self, items: list[SQLModel], update: bool = False) -> None:
        """Insert SQLModel items with upsert-like safety (ignore conflicts)"""
        if not items:
            return

        model_cls = type(items[0])
        table = model_cls.__table__
        values = [item.model_dump() for item in items]

        # HACK: Make dynamic
        if update:
            stmt = insert(table).values(values)
            stmt = stmt.on_conflict_do_update(
                index_elements=["ticker"],
                set_={"category": stmt.excluded.category},  # only update category
            )
        else:
            stmt = insert(table).values(values).on_conflict_do_nothing()

        try:
            with self.engine.begin() as conn:
                conn.execute(stmt)
                conn.commit()
        except SQLAlchemyError as e:
            # Log unexpected DB errors
            logger.exception(f"Failed to insert items into {table.name}: {e}")
            raise

    def insert_items_df(self, df: pd.DataFrame, table_name: str) -> None:
        with self.engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists="append", index=False)

    def fetch_items(self, query: str, params: dict | None = None) -> Sequence[Row[Any]]:
        """Run raw SQL query and return results"""
        with self.engine.connect() as conn:
            result = conn.execute(text(query), parameters=params)
            return result.fetchall()

    def list_tables(self) -> list[str]:
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def delete_ticker(self, model: type[SQLModel], column_name: str, value: str) -> None:
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
