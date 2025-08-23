from pydantic import BaseModel, Field
from datetime import datetime
from typing import Any


class StockTick(BaseModel):
    open: float = Field(..., gt=0.0)
    high: float = Field(..., gt=0.0)
    low: float = Field(..., gt=0.0)
    close: float = Field(..., gt=0.0)
    volume: float = Field(..., gt=0.0)
    timestamp: datetime
    metadata: dict[str, Any] | None = None


class TickSet(BaseModel):
    ticker: str
    tick: list[StockTick]


class KafkaMetadata(BaseModel):
    db: str
    table: str
    ticker: str
    start_time: datetime
    end_time: datetime
    rows: int
