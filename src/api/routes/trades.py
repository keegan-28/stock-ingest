from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from datetime import datetime as dt
import pytz
from src.schema_registry.sql_tables import TradeAction, Tickers
from src.schema_registry.response_models import Trade, TickerCategory
from src.services.database import PostgresDB
from src.api.dependencies import get_db
from src.utils.logger import logger

router = APIRouter(tags=["Trades"])


@router.post("/{ticker}")
def log_trade_action(trade: Trade, db: PostgresDB = Depends(get_db)) -> JSONResponse:
    trade.ticker = trade.ticker.upper()

    trade_entry = TradeAction.model_validate(trade)
    db.insert_items([trade_entry])
    db.insert_items(
        [
            Tickers(
                ticker=trade.ticker,
                last_updated=dt.now(tz=pytz.utc),
                category=TickerCategory.SWINGTRADE,
            )
        ],
        update=True,
    )

    logger.info(f"Logged trade for {trade.ticker}")
    return JSONResponse(
        {"ticker": trade.ticker, "status": "logged"}, status_code=status.HTTP_201_CREATED
    )
