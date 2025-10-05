from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from datetime import datetime as dt, timedelta
import pytz

from src.schema_registry.sql_tables import Tickers, StockTicks, TechnicalFeatures, Correlations
from src.schema_registry.response_models import TickerCategory
from src.transform.pipeline import calculate_indicators, calculate_correlations
from src.utils.logger import logger
from src.services.database import PostgresDB
from src.services.broker import AlpacaBroker
from src.api.dependencies import get_db, get_broker

router = APIRouter(tags=["Tickers"])


@router.get("/")
def get_all_tickers(db: PostgresDB = Depends(get_db)) -> JSONResponse:
    """List all tickers grouped by category."""
    try:
        query = f"SELECT ticker, category FROM {Tickers.__tablename__} ORDER BY category;"
        rows = db.fetch_items(query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    grouped: dict[str, list[str]] = {}
    for ticker, category in rows:
        grouped.setdefault(category, []).append(ticker)
    return JSONResponse(grouped, status_code=status.HTTP_200_OK)


@router.post("/")
def add_ticker(
    ticker: str,
    category: TickerCategory,
    db: PostgresDB = Depends(get_db),
    broker: AlpacaBroker = Depends(get_broker),
) -> JSONResponse:
    """Add a ticker, fetch data, compute indicators + correlations."""
    ticker = ticker.upper()
    logger.info(f"Adding ticker {ticker}")

    last_bar_time = dt.now(tz=pytz.utc) - timedelta(days=5 * 365)
    ticker_data = broker.get_stock_bars_live(
        ticker=ticker, last_bar_time=last_bar_time, time_unit=1, timeframe="Day"
    )

    indicators = calculate_indicators(ticker_data)
    correlations = calculate_correlations(db, StockTicks.__tablename__)

    db.insert_items([Tickers(ticker=ticker, last_updated=dt.now(tz=pytz.utc), category=category)])
    db.insert_items(ticker_data)
    db.insert_items(indicators)
    db.insert_items(correlations)

    return JSONResponse(
        {"ticker": ticker, "category": category}, status_code=status.HTTP_201_CREATED
    )


@router.delete("/{ticker}")
def delete_ticker(ticker: str, db: PostgresDB = Depends(get_db)) -> JSONResponse:
    """Delete a ticker and all its related entries."""
    ticker = ticker.upper()

    for model, column in [
        (Tickers, "ticker"),
        (TechnicalFeatures, "ticker"),
        (Correlations, "ticker_1"),
        (Correlations, "ticker_2"),
        (StockTicks, "ticker"),
    ]:
        db.delete_ticker(model, column, ticker)

    return JSONResponse({"deleted": ticker}, status_code=status.HTTP_200_OK)
