from fastapi import FastAPI, HTTPException
from src.services import services
from src.utils.logger import logger
import os
from datetime import datetime as dt, timedelta
import pytz
from src.transform.pipeline import calculate_indicators, calculate_correlations

app = FastAPI(title="WATCHTOWER", version="0.0.0")

pgdb = services.get_db_conn()
broker = services.get_broker_conn()
broker.connect()

raw_ticker_table = os.environ["DB_TABLE_RAW_DATA"]
indicator_table = os.environ["DB_TABLE_INDICATORS"]
correlation_table = os.environ["DB_TABLE_CORRELATION"]


@app.get("/api/v1/tickers")
def get_all_tickers() -> dict[str, list[str]]:
    try:
        query = f"SELECT DISTINCT ticker FROM {raw_ticker_table}"
        rows = pgdb.fetch_items(query=query)
        if not rows:
            raise HTTPException(status_code=404, detail="No tickers found")
        tickers = [row[0] for row in rows]
        return {"tickers": tickers}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {e}")


@app.post("/api/v1/tickers")
def add_ticker(ticker: str) -> str:
    ticker = ticker.upper()
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

    pgdb.insert_items(raw_ticker_table, ticker_data, conflict_cols=["ticker", "timestamp"])
    pgdb.insert_items(indicator_table, indicators, conflict_cols=["ticker", "timestamp"])
    pgdb.insert_items(
        correlation_table, correlations, conflict_cols=["timestamp", "ticker_1", "ticker_2"]
    )
    return ticker


@app.delete("/api/v1/tickers/{ticker}")
def delete_ticker(ticker: str) -> dict[str, list[str]]:
    ticker = ticker.upper()
    logger.info(f"Removing ticker: {ticker} from all tables.")
    pgdb.delete_ticker(raw_ticker_table, "ticker", ticker)
    pgdb.delete_ticker(indicator_table, "ticker", ticker)
    pgdb.delete_ticker(correlation_table, "ticker_1", ticker)
    pgdb.delete_ticker(correlation_table, "ticker_2", ticker)
    return get_all_tickers()


@app.post("/api/v1/jobs/run_all")
def run_all_tickers():
    # Run Ingest
    # Run Indicators
    # Run Correlation
    pass


@app.post("/api/v1/jobs/{ticker}/run")
def run_single_ticker(ticker: str):
    # Run Ingest
    # Run Indicators
    # Run Correlation
    pass