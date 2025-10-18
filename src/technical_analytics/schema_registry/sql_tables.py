from sqlmodel import SQLModel, Field
from datetime import datetime, date

from src.technical_analytics.schema_registry.response_models import (
    TickerCategory,
    TableSchema,
    Trade,
    ContractType,
    ColumnSchema,
)

# =====================TABLE SCHEMAS============================


class Tickers(SQLModel, table=True):
    ticker: str = Field(primary_key=True, index=True)
    last_updated: datetime
    category: TickerCategory


class StockTicks(SQLModel, table=True):
    ticker: str = Field(primary_key=True, index=True)
    timestamp: datetime = Field(primary_key=True, index=True)

    open: float
    high: float
    low: float
    close: float
    volume: float


class Correlations(SQLModel, table=True):
    ticker_1: str = Field(
        primary_key=True, index=True, description="Stock symbol or instrument identifier"
    )
    ticker_2: str = Field(
        primary_key=True, index=True, description="Stock symbol or instrument identifier"
    )

    correlation: float = Field(..., ge=-1, le=1)


class TechnicalFeatures(SQLModel, table=True):
    ticker: str = Field(
        primary_key=True, index=True, description="Stock symbol or instrument identifier"
    )
    timestamp: datetime = Field(
        primary_key=True, index=True, description="Trading day timestamp (end of day)"
    )

    close: float
    volume: float

    ma_50: float | None = Field(default=None, description="50-day simple moving average of close")
    ma_200: float | None = Field(default=None, description="200-day simple moving average of close")
    rolling_std_50: float | None = Field(
        default=None, description="50-day rolling standard deviation (volatility)"
    )
    ema_50: float | None = Field(
        default=None, description="50-day exponential moving average of close"
    )
    rolling_vol_avg_50: float | None = Field(default=None, description="50-day average of volume")
    rolling_vol_avg_20: float | None = Field(default=None, description="20-day average of volume")

    rsi_14: float | None = Field(default=None, description="14-day Relative Strength Index")

    macd: float | None = Field(default=None, description="MACD line (12 EMA - 26 EMA)")
    macd_signal: float | None = Field(default=None, description="MACD signal line (9 EMA of MACD)")

    bb_upper: float | None = Field(default=None, description="Upper Bollinger Band (20 MA + 2*std)")
    bb_lower: float | None = Field(default=None, description="Lower Bollinger Band (20 MA - 2*std)")


class TradeAction(Trade, SQLModel, table=True):
    ticker: str = Field(index=True, primary_key=True)
    trade_date: date = Field(index=True, primary_key=True)


class MonteCarloSummary(SQLModel, table=True):
    # Composite primary key: (ticker, timestamp)
    ticker: str = Field(primary_key=True, index=True)
    timestamp: datetime = Field(primary_key=True, index=True)

    # Aggregated results
    mean_price: float
    p5: float
    p10: float
    p25: float
    p75: float
    p90: float
    p95: float


class OptionChain(SQLModel, table=True):
    ticker: str
    option_symbol: str = Field(primary_key=True)
    contract_type: ContractType

    strike_price: float
    expiration_date: datetime

    trade_timestamp: datetime
    trade_price: float
    trade_size: float

    quote_timestamp: datetime
    quote_bid_price: float
    quote_ask_price: float
    quote_bid_size: float

    implied_volatility: float

    delta: float = Field(..., ge=-1.0, le=1.0)
    gamma: float = Field(..., ge=0.0)
    theta: float
    vega: float = Field(..., gt=0.0)
    rho: float


# ===================================METHODS==============================


def get_table_schema(model: type[SQLModel]) -> TableSchema:
    table = model.__table__
    columns = [
        ColumnSchema(name=col.name, type_=str(col.type), is_primary_key=col.primary_key)
        for col in table.columns
    ]
    return TableSchema(name=table.name, columns=columns)
