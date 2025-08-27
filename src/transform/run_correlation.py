from src.strategies.strategies import TechnicalIndicators
from src.common.schema_registry import Correlation
from src.utils.utils import load_config
from src.common.schema_registry import StockTick
import polars as pl
from src.services import services
import os


correlation_table = os.getenv("DB_TABLE_CORRELATIONS")

pgdb = services.get_db_conn()

if not pgdb.table_exists(correlation_table):
    pgdb.create_table(correlation_table, Correlation, ["timestamp", "ticker_1","ticker_2"])

config = load_config("config/tickers/tickers.yaml")
tickers = config["tickers"]

columns = StockTick.model_fields.keys()


raw_data = pgdb.fetch_items(
    table_name="raw_data",
    query="""
        SELECT *
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) AS rn
            FROM raw_data
        ) sub
        WHERE rn <= 20
        ORDER BY ticker, timestamp;
    """
)

df = pl.DataFrame(raw_data)
df = TechnicalIndicators.rolling_correlation(df, tickers)

df_dict = df.to_dicts()
correlations = []
for row in df_dict:

    correlations.append(
        Correlation(
            timestamp=row.get("timestamp"),
            ticker_1=row.get("ticker1"),
            ticker_2=row.get("ticker2"),
            correlation=row.get(correlation_table)
        )
    )

pgdb.insert_items(correlation_table, correlations, conflict_cols=["timestamp", "ticker_1", "ticker_2"])
