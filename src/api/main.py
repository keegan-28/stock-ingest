from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from src.services import services
from src.utils.logger import logger
from sqlmodel import SQLModel, Table as SQLTable
from datetime import datetime as dt, timedelta
import pytz
from src.transform.pipeline import calculate_indicators, calculate_correlations
from src.common.schema_registry import (
    StockTicks,
    TechnicalFeatures,
    Correlations,
    Tickers,
    TickerCategory,
    get_table_schema,
    TableSchema,
    OptionSnapshot
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


# ticker_table = os.environ["DB_TABLE_TICKERS"]
# raw_ticker_table = os.environ["DB_TABLE_RAW_DATA"]
# indicator_table = os.environ["DB_TABLE_INDICATORS"]
# correlation_table = os.environ["DB_TABLE_CORRELATION"]
# options_table = os.environ["DB_OPTIONS_TABLE"]

TABLE_MAPPING: set[SQLModel] = {
    Tickers,
    StockTicks,
    TechnicalFeatures,
    Correlations,
    # OptionSnapshot
}


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/api/v1/docs")


@app.post("/tables/create")
def create_tables() -> list[str]:
    tables_created: list[str] = []
    for model in TABLE_MAPPING:
        if not pgdb.table_exists(model.__tablename__):
            pgdb.create_table(model)
            tables_created.append(model.__tablename__)
    return tables_created


@app.get("/tables", response_model=list[TableSchema])
def get_table_schemas() -> list[TableSchema]:
    return [get_table_schema(model) for model in TABLE_MAPPING]


@app.get("/tickers")
def get_all_tickers() -> dict[str, list[str]]:
    try:
        query = f"SELECT ticker, category FROM {Tickers.__tablename__} ORDER BY category;"
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
    ticker_row = Tickers(ticker=ticker, last_updated=dt.now(tz=pytz.utc), category=category)
    # Run ingest + indicators + correlation
    logger.info(f"Retrieving data for ticker: {ticker}")
    last_bar_time = dt.now(tz=pytz.utc) - timedelta(days=5 * 365)
    ticker_data = broker.get_stock_bars_live(
        ticker=ticker, last_bar_time=last_bar_time, time_unit=1, timeframe="Day"
    )
    # options = broker.get_options_chains(ticker)

    # Run Indicators
    indicators = calculate_indicators(ticker_data=ticker_data)

    # Run correlations
    correlations = calculate_correlations(pgdb, StockTicks.__tablename__)

    pgdb.insert_items([ticker_row])
    pgdb.insert_items(ticker_data)
    pgdb.insert_items(indicators)
    pgdb.insert_items(correlations)
    # pgdb.insert_items(options_table, options)
    return ticker


@app.delete("/tickers/{ticker}")
def delete_ticker(ticker: str) -> dict[str, list[str]]:
    ticker = ticker.upper()
    logger.info(f"Removing ticker: {ticker} from all tables.")
    pgdb.delete_ticker(Tickers, "ticker", ticker)
    pgdb.delete_ticker(TechnicalFeatures, "ticker", ticker)
    pgdb.delete_ticker(Correlations, "ticker_1", ticker)
    pgdb.delete_ticker(Correlations, "ticker_2", ticker)
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


# @app.post("/jobs/{ticker}/run")
# def run_single_ticker(ticker: str):
#     logger.info(f"Retrieving data for ticker: {ticker}")
#     last_bar_time = dt.now(tz=pytz.utc) - timedelta(days=5 * 365)
#     ticker_data = broker.get_stock_bars_live(
#         ticker=ticker, last_bar_time=last_bar_time, time_unit=1, timeframe="Day"
#     )

#     # Run Indicators
#     indicators = calculate_indicators(ticker_data=ticker_data)

#     # Run correlations
#     correlations = calculate_correlations(pgdb, raw_ticker_table)
#     pgdb.insert_items(raw_ticker_table, ticker_data)
#     pgdb.insert_items(indicator_table, indicators)
#     pgdb.insert_items(correlation_table, correlations)
#     return ticker
