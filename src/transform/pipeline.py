from src.schema_registry.sql_tables import TechnicalFeatures, StockTicks, Correlations
from src.strategies.strategies import (
    rolling_mean,
    rolling_std,
    rsi,
    macd,
    bollinger_bands,
    rolling_volume_avg,
    rolling_correlation,
)
import polars as pl
from typing import Any
import numpy as np
from src.services.database import PostgresDB


def calculate_indicators(ticker_data: list[StockTicks]) -> list[TechnicalFeatures]:
    df = pl.DataFrame([item.model_dump() for item in ticker_data])
    df = rolling_mean(df, 50, "close", "ma_50")
    df = rolling_mean(df, 200, "close", "ma_200")
    df = rolling_std(df, 50, "close", "rolling_std_50")
    df = rolling_volume_avg(df, 50, "rolling_vol_avg_50")
    df = rsi(df, "close", 14)
    df = macd(df)
    df = bollinger_bands(df, 20, "bb")

    df_dict = df.to_dicts()
    indicators = []

    for row in df_dict:

        def safe_float(value: Any) -> float:
            if value is None or (isinstance(value, float) and np.isnan(value)):
                return 0.0
            return float(round(value, 3))

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
        indicators.append(features)
    return indicators


def calculate_correlations(
    pgdb: PostgresDB, raw_ticker_table: str, window: int = 90
) -> list[Correlations]:
    """
    Calculate rolling correlations for one ticker vs others.

    Args:
        ticker: single ticker to process
        pgdb: database connection (with .fetch_items)
        raw_ticker_table: table name with raw data
        tickers: list of all tickers in portfolio/watchlist
        window: number of most recent rows to use

    Returns:
        List of Correlation objects
    """

    # Fetch last N rows for the chosen ticker
    tickers = pgdb.fetch_items(query=f"SELECT DISTINCT ticker FROM {raw_ticker_table}")
    tickers = [t[0] for t in tickers]
    raw_data = pgdb.fetch_items(
        query=f"""
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY timestamp DESC) AS rn
                FROM {raw_ticker_table}
            ) sub
            WHERE rn <= {window}
            ORDER BY ticker, timestamp;
        """,
    )
    if not raw_data:
        return []

    df = pl.DataFrame(raw_data)
    df = rolling_correlation(df, tickers, window=window)

    # Convert to Correlation models
    return [
        Correlations(
            ticker_1=row.get("ticker1"),
            ticker_2=row.get("ticker2"),
            correlation=row.get("correlation"),
        )
        for row in df.to_dicts()
    ]
