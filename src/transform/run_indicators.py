from src.services import services
from src.common.schema_registry import TechnicalFeatures
from src.utils.utils import load_config
from src.common.schema_registry import StockTick
from src.strategies.strategies import TechnicalIndicators
import polars as pl
import numpy as np
import os


indicator_table = os.getenv("DB_TABLE_INDICATORS")

pgdb = services.get_db_conn()

if not pgdb.table_exists(indicator_table):
    pgdb.create_table(indicator_table, TechnicalFeatures, ["timestamp", "ticker"])

config = load_config("config/tickers/tickers.yaml")
tickers = config["tickers"]

columns = StockTick.model_fields.keys()

for ticker in tickers:
    raw_data = pgdb.fetch_items(
        table_name="raw_data",
        query="SELECT * FROM raw_data WHERE ticker = :ticker ORDER BY timestamp",
        params={"ticker": ticker},
    )
    df = pl.DataFrame(raw_data)
    df = TechnicalIndicators.rolling_mean(df, 50, "close", "ma_50")
    df = TechnicalIndicators.rolling_mean(df, 200, "close", "ma_200")
    df = TechnicalIndicators.rolling_std(df, 50, "close", "rolling_std_50")
    df = TechnicalIndicators.rolling_volume_avg(df, 50, "rolling_vol_avg_50")
    df = TechnicalIndicators.rsi(df, "close", 14)
    df = TechnicalIndicators.macd(df)
    df = TechnicalIndicators.bollinger_bands(df, 20, "bb")

    df_dict = df.to_dicts()
    features_list = []

    for row in df_dict:

        def safe_float(value):
            if value is None or (isinstance(value, float) and np.isnan(value)):
                return 0.0
            return float(value)

        features = TechnicalFeatures(
            ticker=row["ticker"],
            timestamp=row["timestamp"],
            close=safe_float(row.get("close")),
            ma_50=safe_float(row.get("ma_50")),
            ma_200=safe_float(row.get("ma_200")),
            rolling_std_50=safe_float(row.get("rolling_std_50")),
            rolling_vol_avg_50=safe_float(row.get("rolling_vol_avg_50")),
            rsi_14=safe_float(row.get("rsi_14")),
            macd=safe_float(row.get("macd")),
            macd_signal=safe_float(row.get("macd_signal")),
            bb_upper=safe_float(row.get("bb_upper")),
            bb_lower=safe_float(row.get("bb_lower")),
        )
        features_list.append(features)

    pgdb.insert_items(indicator_table, features_list, conflict_cols=["ticker", "timestamp"])
