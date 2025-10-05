from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from sqlmodel import SQLModel
from src.schema_registry.sql_tables import (
    StockTicks,
    TechnicalFeatures,
    Correlations,
    Tickers,
    TradeAction,
    get_table_schema,
)
from src.schema_registry.response_models import TableSchema
from src.services.database import PostgresDB
from src.api.dependencies import get_db

router = APIRouter(tags=["Tables"])

TABLE_MAPPING: set[type[SQLModel]] = {
    Tickers,
    StockTicks,
    TechnicalFeatures,
    Correlations,
    TradeAction,
}


@router.post("/create", response_model=list[str])
def create_tables(db: PostgresDB = Depends(get_db)) -> JSONResponse:
    """Create tables if they don't exist."""
    created = []
    for model in TABLE_MAPPING:
        if not db.table_exists(model.__tablename__):
            db.create_table(model)
            created.append(model.__tablename__)
    return JSONResponse({"created": created}, status_code=status.HTTP_201_CREATED)


@router.get("/", response_model=list[TableSchema])
def get_table_schemas():
    """Return schemas for all tables."""
    return [get_table_schema(model) for model in TABLE_MAPPING]
