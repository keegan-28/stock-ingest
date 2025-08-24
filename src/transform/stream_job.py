from src.ingest.broker import AlpacaBroker
from src.common.database import PostgresDB
from src.common.schema_registry import StockTick
from src.common.logger import logger
from datetime import datetime as dt
import pytz


# Instantiate kafka client
# Instantiate database connection
# Broker API connection

# Retrieve data
# Write to DB
# Send kafka message with metadata

broker = AlpacaBroker(
    api_key="test",
    api_secret="test",
)
broker.connect()

pgdb = PostgresDB("myuser", "mypassword", "localhost", 5432, "stockdb")
if not pgdb.table_exists("stockdb"):
    pgdb.create_table("raw_data", StockTick, ["timestamp", "ticker"])

try:
    last_bar_time = dt(2025, 1, 1, tzinfo=pytz.utc)
    ticker_data = broker.get_stock_bars_live(
        ticker="AAPL", last_bar_time=last_bar_time, time_unit=1, timeframe="Minute"
    )
    logger.info(f"Retrieved {len(ticker_data)}")
except Exception as e:
    logger.error(e)
    raise e

pgdb.insert_items("raw_data", ticker_data, conflict_cols=["ticker", "timestamp"])
