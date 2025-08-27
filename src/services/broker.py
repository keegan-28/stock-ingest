from alpaca.data import (
    StockHistoricalDataClient,
    SupportedCurrencies,
    TimeFrameUnit,
    TimeFrame,
)
from alpaca.data import StockBarsRequest, BarSet, Bar
from datetime import datetime

from src.common.schema_registry import StockTick


class AlpacaBroker:
    def __init__(self, api_key: str, api_secret: str) -> None:
        self.__api_key = api_key
        self.__api_secret = api_secret

        self.data_client = None

    def connect(self) -> None:
        self.data_client = StockHistoricalDataClient(
            api_key=self.__api_key, secret_key=self.__api_secret
        )

    def close_connection(self) -> None:
        pass

    def get_stock_bars_live(
        self,
        ticker: str,
        last_bar_time: datetime,
        time_unit: int = 1,
        timeframe: str = "Minute",
    ) -> list[StockTick]:
        if self.data_client is None:
            raise AssertionError("Client not set.")

        timeframes = {
            "Minute": TimeFrame(time_unit, TimeFrameUnit.Minute),
            "Hour": TimeFrame(time_unit, TimeFrameUnit.Hour),
            "Day": TimeFrame(time_unit, TimeFrameUnit.Day),
            "Week": TimeFrame(time_unit, TimeFrameUnit.Week),
            "Month": TimeFrame(time_unit, TimeFrameUnit.Month),
        }

        request_params = StockBarsRequest(
            symbol_or_symbols=ticker,
            start=last_bar_time,
            currency=SupportedCurrencies.USD,
            timeframe=timeframes[timeframe],
            adjustment="split",
        )
        raw_data: BarSet = self.data_client.get_stock_bars(request_params=request_params)
        data: list[StockTick] = []
        raw_ticks: list[Bar] = raw_data[ticker]
        for bar in raw_ticks:
            if bar.timestamp > last_bar_time:
                data.append(
                    StockTick(
                        ticker=bar.symbol,
                        open=bar.open,
                        high=bar.high,
                        low=bar.low,
                        close=bar.close,
                        volume=bar.volume,
                        timestamp=bar.timestamp,
                    )
                )
        return data
