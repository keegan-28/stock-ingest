from alpaca.data import (
    StockHistoricalDataClient,
    SupportedCurrencies,
    TimeFrameUnit,
    TimeFrame,
    OptionHistoricalDataClient,
    OptionChainRequest,
    OptionsSnapshot,
    ContractType,
)
from alpaca.data import StockBarsRequest, BarSet, Bar
from datetime import datetime, timedelta
import pytz
from src.common.schema_registry import StockTicks, ContractType, OptionSnapshot, Greeks
from src.utils.logger import logger


class AlpacaBroker:
    def __init__(self, api_key: str, api_secret: str) -> None:
        self.__api_key = api_key
        self.__api_secret = api_secret

        self.data_client = None
        self.options_client = None

    def connect(self) -> None:
        self.data_client = StockHistoricalDataClient(
            api_key=self.__api_key, secret_key=self.__api_secret
        )
        self.options_client = OptionHistoricalDataClient(
            api_key=self.__api_key, secret_key=self.__api_secret
        )

    def close_connection(self) -> None:
        self.data_client = None
        self.options_client = None

    def get_stock_bars_live(
        self,
        ticker: str,
        last_bar_time: datetime,
        time_unit: int = 1,
        timeframe: str = "Minute",
    ) -> list[StockTicks]:
        if self.data_client is None:
            raise AssertionError("Client not set.")

        timeframes = {
            "Minute": TimeFrame(time_unit, TimeFrameUnit.Minute),
            "Hour": TimeFrame(time_unit, TimeFrameUnit.Hour),
            "Day": TimeFrame(time_unit, TimeFrameUnit.Day),
            "Week": TimeFrame(time_unit, TimeFrameUnit.Week),
            "Month": TimeFrame(time_unit, TimeFrameUnit.Month),
        }
        logger.info(timeframes[timeframe])
        request_params = StockBarsRequest(
            symbol_or_symbols=ticker,
            start=last_bar_time,
            currency=SupportedCurrencies.USD,
            timeframe=timeframes[timeframe],
            adjustment="split",
        )
        raw_data: BarSet = self.data_client.get_stock_bars(request_params=request_params)
        data: list[StockTicks] = []
        raw_ticks: list[Bar] = raw_data[ticker]
        for bar in raw_ticks:
            if bar.timestamp > last_bar_time:
                data.append(
                    StockTicks(
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

    def get_options_chains(
        self,
        ticker: str,
        weeks_gte: int = 3,
        weeks_lte: int = 4,
    ) -> list[OptionSnapshot]:
        def extract_strike_price(symbol: str):
            strike_str = symbol[-8:]
            return float(f"{int(strike_str[:-3])}.{strike_str[-3:]}")

        now = datetime.now(pytz.utc)
        data: list[OptionSnapshot] = []
        for c_type in (ContractType.PUT, ContractType.CALL):
            options = self.options_client.get_option_chain(
                request_params=OptionChainRequest(
                    underlying_symbol=ticker,
                    feed="indicative",
                    type=c_type,
                    expiration_date_gte=now.date() + timedelta(weeks=weeks_gte),
                    expiration_date_lte=now.date() + timedelta(weeks=weeks_lte),
                )
            )   

            for key, model in options.items():
                assert isinstance(model, OptionsSnapshot)
                try:
                    data.append(
                        OptionSnapshot(
                            ticker=ticker,
                            option_symbol=key,
                            contract_type=c_type,
                            strike_price=extract_strike_price(key),
                            expiration_date=datetime.strptime(key[4:10], '%y%m%d'),
                            trade_timestamp=model.latest_trade.timestamp,
                            trade_size=model.latest_trade.size,
                            trade_price=model.latest_trade.price,
                            quote_ask_price=model.latest_quote.ask_price,
                            quote_bid_price=model.latest_quote.bid_price,
                            quote_bid_size=model.latest_quote.bid_size,
                            quote_timestamp=model.latest_quote.timestamp,
                            implied_volatility=model.implied_volatility,
                            greeks=Greeks(
                                delta=model.greeks.delta,
                                gamma=model.greeks.gamma,
                                theta=model.greeks.theta,
                                vega=model.greeks.vega,
                                rho=model.greeks.rho,
                            ),
                        )
                    )
                except AttributeError:
                    pass
        return data
