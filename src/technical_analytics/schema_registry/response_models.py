from pydantic import BaseModel, Field
from datetime import date
from enum import Enum


class TickerCategory(str, Enum):
    PORTFOLIO = "portfolio"
    WATCHLIST = "watchlist"
    SWINGTRADE = "swingtrade"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class ContractType(str, Enum):
    CALL = "call"
    PUT = "put"


class ColumnSchema(BaseModel):
    name: str
    type_: str
    is_primary_key: bool


class TableSchema(BaseModel):
    name: str
    columns: list[ColumnSchema]


class Trade(BaseModel):
    ticker: str
    trade_date: date
    order_side: OrderSide
    qty: float
    price: float
    fees: float = Field(default=0.0)

    def __str__(self):
        return f"Ticker: {self.ticker} | Order Side: {self.order_side} | QTY: {self.qty} | Price: {self.price}."
