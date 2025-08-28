import polars as pl
import pandas as pd


class TechnicalIndicators:
    @staticmethod
    def rolling_mean(
        df: pl.DataFrame,
        window_size: int,
        col_name: str = "close",
        output_col: str | None = None,
    ) -> pl.DataFrame:
        if output_col is None:
            output_col = f"ma{window_size}"
        return df.with_columns(pl.col(col_name).rolling_mean(window_size).alias(output_col))

    @staticmethod
    def rolling_std(
        df: pl.DataFrame,
        window_size: int,
        col_name: str = "close",
        output_col: str | None = None,
    ) -> pl.DataFrame:
        if output_col is None:
            output_col = f"std{window_size}"
        return df.with_columns(pl.col(col_name).rolling_std(window_size).alias(output_col))

    @staticmethod
    def rolling_volume_avg(
        df: pl.DataFrame, window_size: int, output_col: str | None = None
    ) -> pl.DataFrame:
        if output_col is None:
            output_col = f"vol_ma{window_size}"
        return df.with_columns(pl.col("volume").rolling_mean(window_size).alias(output_col))

    @staticmethod
    def macd(df: pl.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pl.DataFrame:
        df = TechnicalIndicators.rolling_mean(df, fast, "close", "ema_fast")
        df = TechnicalIndicators.rolling_mean(df, slow, "close", "ema_slow")
        df = df.with_columns((pl.col("ema_fast") - pl.col("ema_slow")).alias("macd"))
        df = TechnicalIndicators.rolling_mean(df, signal, "macd", "macd_signal")
        return df

    @staticmethod
    def bollinger_bands(
        df: pl.DataFrame, window_size: int = 20, output_prefix: str = "bb"
    ) -> pl.DataFrame:
        df = TechnicalIndicators.rolling_mean(df, window_size, "close", f"{output_prefix}_ma")
        df = TechnicalIndicators.rolling_std(df, window_size, "close", f"{output_prefix}_std")
        df = df.with_columns(
            [
                (pl.col(f"{output_prefix}_ma") + 2 * pl.col(f"{output_prefix}_std")).alias(
                    f"{output_prefix}_upper"
                ),
                (pl.col(f"{output_prefix}_ma") - 2 * pl.col(f"{output_prefix}_std")).alias(
                    f"{output_prefix}_lower"
                ),
            ]
        )
        return df

    @staticmethod
    def rsi(
        df: pl.DataFrame,
        price_col: str = "close",
        period: int = 14,
        col_name: str | None = None,
    ) -> pl.DataFrame:
        if col_name is None:
            col_name = f"rsi_{period}"
        return (
            df.with_columns([(pl.col(price_col).diff()).alias("price_change")])
            .with_columns(
                [
                    pl.when(pl.col("price_change") > 0)
                    .then(pl.col("price_change"))
                    .otherwise(0.0)
                    .alias("gain"),
                    pl.when(pl.col("price_change") < 0)
                    .then(-pl.col("price_change"))
                    .otherwise(0.0)
                    .alias("loss"),
                ]
            )
            .with_columns(
                [
                    # Calculate average gain and loss using EMA (exponential moving average)
                    pl.col("gain").ewm_mean(span=period).alias("avg_gain"),
                    pl.col("loss").ewm_mean(span=period).alias("avg_loss"),
                ]
            )
            .with_columns(
                [
                    # Calculate RS (Relative Strength)
                    (pl.col("avg_gain") / pl.col("avg_loss")).alias("rs")
                ]
            )
            .with_columns(
                [
                    # Calculate RSI
                    (100.0 - (100.0 / (1.0 + pl.col("rs")))).alias(col_name)
                ]
            )
            .drop(["price_change", "gain", "loss", "avg_gain", "avg_loss", "rs"])
        )

    @staticmethod
    def rolling_correlation(df: pl.DataFrame, tickers: list[str], window: int = 20) -> pl.DataFrame:
        """
        Calculate rolling correlations between multiple tickers.
        Returns a long-format Polars DataFrame: timestamp | ticker1 | ticker2 | correlation
        """
        # Pivot to wide format
        df_wide = df.pivot(values="close", index="timestamp", columns="ticker")

        # Compute returns
        returns = df_wide.select(
            [(pl.col(col).pct_change().alias(col)) for col in tickers] + [pl.col("timestamp")]
        )
        assert isinstance(returns, pl.DataFrame)

        # Convert to Pandas for rolling correlation
        returns_pd = returns.to_pandas().set_index("timestamp")
        rolling_corrs = []

        for start in range(len(returns_pd) - window + 1):
            window_df = returns_pd.iloc[start : start + window]
            corr_matrix = window_df.corr()
            corr_matrix["timestamp"] = returns_pd.index[start + window - 1]
            rolling_corrs.append(corr_matrix)

        rolling_corr_df = pd.concat(rolling_corrs)
        rolling_corr_df = (
            rolling_corr_df.reset_index()
            .melt(id_vars=["timestamp", "index"], var_name="ticker2", value_name="correlation")
            .rename(columns={"index": "ticker1"})
        )

        return pl.from_pandas(rolling_corr_df)
