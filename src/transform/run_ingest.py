from src.services import services
from src.common.schema_registry import StockTick
from src.utils.logger import logger
from datetime import datetime as dt, timedelta
import pytz
import os
from src.utils.utils import load_config


broker = services.get_broker_conn()
broker.connect()
pgdb = services.get_db_conn()

raw_ticker_table = os.environ["DB_TABLE_RAW_DATA"]

if not pgdb.table_exists(raw_ticker_table):
    pgdb.create_table(raw_ticker_table, StockTick, ["timestamp", "ticker"])

config = load_config(os.environ["TICKER_CONFIG"])
tickers = config["tickers"]
ticker_data = []
try:
    for ticker in tickers:
        logger.info(f"Retrieving data for ticker: {ticker}")
        last_bar_time = dt.now(tz=pytz.utc) - timedelta(days=5 * 365)
        ticker_data.extend(
            broker.get_stock_bars_live(
                ticker=ticker, last_bar_time=last_bar_time, time_unit=1, timeframe="Day"
            )
        )
except Exception as e:
    logger.error(e)
    raise e


pgdb.insert_items(raw_ticker_table, ticker_data, conflict_cols=["ticker", "timestamp"])
logger.info(f"Inserted {len(ticker_data)} into database.")
