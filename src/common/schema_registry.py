from pydantic import BaseModel, Field
from datetime import datetime
from typing import Any


class StockTick(BaseModel):
    ticker: str
    open: float = Field(..., gte=0.0)
    high: float = Field(..., gte=0.0)
    low: float = Field(..., gte=0.0)
    close: float = Field(..., gte=0.0)
    volume: float = Field(..., gte=0.0)
    timestamp: datetime
    metadata: dict[str, Any] | None = None


class Correlation(BaseModel):    
    timestamp: datetime = Field(..., description="Trading day timestamp (end of day)")

    ticker_1: str = Field(..., description="Stock symbol or instrument identifier")
    ticker_2: str = Field(..., description="Stock symbol or instrument identifier")

    correlation: float = Field(..., ge=-1, le=1)


class TechnicalFeatures(BaseModel):
    ticker: str = Field(..., description="Stock symbol or instrument identifier")
    timestamp: datetime = Field(..., description="Trading day timestamp (end of day)")

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
