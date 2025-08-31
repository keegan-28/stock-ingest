from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from src.services import services
from src.utils.logger import logger
import os
from datetime import datetime as dt, timedelta
import pytz
from src.transform.pipeline import calculate_indicators, calculate_correlations
from src.common.schema_registry import (
    StockTick,
    TechnicalFeatures,
    Correlation,
    TickerTable,
    TickerCategory,
    get_table_schema,
    Table,
)
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Do Nothing on startup
    yield
    pgdb.engine.dispose(close=True)
    broker.close_connection()
    logger.info("Closed all connections")


app = FastAPI(
    title="BANKVAULT",
    root_path="/api/v1",
    openapi_url="/openapi.json",
    docs_url="/docs",
    version="0.0.0",
    lifespan=lifespan,
)

pgdb = services.get_db_conn()
broker = services.get_broker_conn()
broker.connect()


ticker_table = os.environ["DB_TABLE_TICKERS"]
raw_ticker_table = os.environ["DB_TABLE_RAW_DATA"]
indicator_table = os.environ["DB_TABLE_INDICATORS"]
correlation_table = os.environ["DB_TABLE_CORRELATION"]

TABLE_MAPPING = {
    ticker_table: TickerTable,
    raw_ticker_table: StockTick,
    indicator_table: TechnicalFeatures,
    correlation_table: Correlation
}


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/api/v1/docs")


@app.post("/tables/create")
def create_tables() -> list[str]:
    tables_created: list[str] = []
    for name, model in TABLE_MAPPING.items():
        if pgdb.table_exists(name):
            pgdb.create_table(name, model)
            tables_created.append(name)
    return tables_created


@app.get("/tables", response_model=list[Table])
def get_table_schemas() -> list[Table]:
    return [
        get_table_schema(name, model) for name, model in TABLE_MAPPING.items()
    ]


@app.get("/tickers")
def get_all_tickers() -> dict[str, list[str]]:
    try:
        query = f"SELECT ticker, category FROM {ticker_table} ORDER BY category;"
        rows = pgdb.fetch_items(query=query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

    if not rows:
        logger.warning("No Tickers Found.")
        return {}

    grouped_tickers: dict[str, list[str]] = {}
    for ticker, category in rows:
        if category not in grouped_tickers:
            grouped_tickers[category] = []
        grouped_tickers[category].append(ticker)

    return grouped_tickers


@app.post("/tickers")
def add_ticker(ticker: str, category: TickerCategory) -> str:
    ticker = ticker.upper()
    ticker_row = TickerTable(ticker=ticker, last_updated=dt.now(tz=pytz.utc), category=category)
    # Run ingest + indicators + correlation
    logger.info(f"Retrieving data for ticker: {ticker}")
    last_bar_time = dt.now(tz=pytz.utc) - timedelta(days=5 * 365)
    ticker_data = broker.get_stock_bars_live(
        ticker=ticker, last_bar_time=last_bar_time, time_unit=1, timeframe="Day"
    )

    # Run Indicators
    indicators = calculate_indicators(ticker_data=ticker_data)

    # Run correlations
    correlations = calculate_correlations(pgdb, raw_ticker_table)

    pgdb.insert_items(ticker_table, [ticker_row])
    pgdb.insert_items(raw_ticker_table, ticker_data)
    pgdb.insert_items(indicator_table, indicators)
    pgdb.insert_items(correlation_table, correlations)
    return ticker


@app.delete("/tickers/{ticker}")
def delete_ticker(ticker: str) -> dict[str, list[str]]:
    ticker = ticker.upper()
    logger.info(f"Removing ticker: {ticker} from all tables.")
    pgdb.delete_ticker(raw_ticker_table, "ticker", ticker)
    pgdb.delete_ticker(indicator_table, "ticker", ticker)
    pgdb.delete_ticker(correlation_table, "ticker_1", ticker)
    pgdb.delete_ticker(correlation_table, "ticker_2", ticker)
    return get_all_tickers()


@app.post("/jobs/run_all")
def run_all_tickers():
    # TODO: Use ThreadPool
    completed_tickers = []

    for category, tickers in get_all_tickers().items():
        for ticker in tickers:
            try:
                _ = add_ticker(ticker, TickerCategory(category))
                completed_tickers.append(ticker)
            except Exception as e:
                logger.error(f"{ticker}: Internal server error: {e}")

    return completed_tickers


@app.post("/jobs/{ticker}/run")
def run_single_ticker(ticker: str):
    logger.info(f"Retrieving data for ticker: {ticker}")
    last_bar_time = dt.now(tz=pytz.utc) - timedelta(days=5 * 365)
    ticker_data = broker.get_stock_bars_live(
        ticker=ticker, last_bar_time=last_bar_time, time_unit=1, timeframe="Day"
    )

    # Run Indicators
    indicators = calculate_indicators(ticker_data=ticker_data)

    # Run correlations
    correlations = calculate_correlations(pgdb, raw_ticker_table)
    pgdb.insert_items(raw_ticker_table, ticker_data)
    pgdb.insert_items(indicator_table, indicators)
    pgdb.insert_items(correlation_table, correlations)
    return 
