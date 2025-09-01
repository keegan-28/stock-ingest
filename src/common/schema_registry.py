from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum
from typing import get_args, get_origin, Union

# =====================TABLE SCHEMAS============================


class TickerCategory(str, Enum):
    PORTFOLIO = "portfolio"
    WATCHLIST = "watchlist"


class TickerTable(BaseModel):
    ticker: str = Field(..., primary_key=True)
    last_updated: datetime
    category: TickerCategory


class StockTick(BaseModel):
    ticker: str = Field(..., primary_key=True)
    timestamp: datetime = Field(..., primary_key=True)
    open: float
    high: float
    low: float
    close: float
    volume: float


class Correlation(BaseModel):
    ticker_1: str = Field(
        ..., description="Stock symbol or instrument identifier", primary_key=True
    )
    ticker_2: str = Field(
        ..., description="Stock symbol or instrument identifier", primary_key=True
    )

    correlation: float = Field(..., ge=-1, le=1)


class TechnicalFeatures(BaseModel):
    ticker: str = Field(..., description="Stock symbol or instrument identifier", primary_key=True)
    timestamp: datetime = Field(
        ..., description="Trading day timestamp (end of day)", primary_key=True
    )

    close: float

    ma_50: float | None = Field(None, description="50-day simple moving average of close")
    ma_200: float | None = Field(None, description="200-day simple moving average of close")
    rolling_std_50: float | None = Field(
        None, description="50-day rolling standard deviation (volatility)"
    )
    rolling_vol_avg_50: float | None = Field(None, description="50-day average of volume")

    rsi_14: float | None = Field(None, description="14-day Relative Strength Index")

    macd: float | None = Field(None, description="MACD line (12 EMA - 26 EMA)")
    macd_signal: float | None = Field(None, description="MACD signal line (9 EMA of MACD)")

    bb_upper: float | None = Field(None, description="Upper Bollinger Band (20 MA + 2*std)")
    bb_lower: float | None = Field(None, description="Lower Bollinger Band (20 MA - 2*std)")


# ====================================API RETURN SCHEMAS=====================================


class Column(BaseModel):
    name: str
    column_type: str
    indexed: bool = False


class Table(BaseModel):
    table_name: str
    columns: list[Column]


# ===================================METHODS==============================


def get_table_schema(table_name: str, model: BaseModel) -> Table:
    columns = []

    for field_name, field_info in model.model_fields.items():
        is_primary_key = False
        if field_info.json_schema_extra and "primary_key" in field_info.json_schema_extra:
            is_primary_key = True
        # Determine the base column type, correctly handling unions
        field_type = field_info.annotation
        origin = get_origin(field_type)

        # Check if it's a Union type, including UnionType from Python 3.10+
        if origin is Union or str(origin) == "<class 'types.UnionType'>":
            union_args = get_args(field_type)
            # Find the first non-None type in the union
            for arg in union_args:
                if arg is not type(None):
                    column_type = arg.__name__
                    break
            else:
                # Fallback for unions that might only contain None
                column_type = "Any"
        else:
            column_type = field_type.__name__

        columns.append(
            Column(name=field_name, column_type=column_type, is_primary_key=is_primary_key)
        )

    return Table(table_name=table_name, columns=columns)
