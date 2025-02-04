from dataclasses import dataclass
from typing import Optional, Literal
from imports import *
from functools import wraps
from types import SimpleNamespace


class Equity:
    __slots__ = ("indicators", "bt_object", "ticker", "timeframe", "name")

    def __init__(self, ticker: str, bt_object: object, timeframe, name):
        self.name = name
        self.timeframe = timeframe
        self.ticker = ticker
        self.bt_object = bt_object
        self.indicators = {}  # Dict of indicators. Includes OHLCV.

    def updateIndicators(self, cur_timestamp: pl.Datetime) -> None:
        for _, ind in self.indicators.items():
            ind.getKnownData(cur_timestamp)
            print("know data fetched")

    def addIndicator(self, df, col_name, name):
        """Usage:\n
        Equity.name[index]\n
        Ex: SPY.close[1]\n

        :param df: dataframe containing timestamp and indicator columns.
        :param col_name: name of the indicator column indide the column.
        :param name: name of indicator for refrence.
        """
        indicator = Indicator(df.select("timestamp", col_name), self.timeframe, name)
        self.indicators[name] = indicator

    def __getattribute__(self, name: str):
        """Dot notation access: equity.close"""
        indicators = super().__getattribute__("indicators")
        if name in indicators.keys():
            return indicators[name]
        return super().__getattribute__(name)

class Indicator:
    __slots__ = ("df", "timeframe", "name", "known_data", "name")

    def __init__(self, df: pl.DataFrame, timeframe: pl.Datetime, name: str):
        self.df = df
        self.timeframe = timeframe
        self.name = name

    def getKnownData(self, timestamp) -> None:
        curr_and_past_data = self.df.filter(pl.col("timestamp") <= timestamp)
        self.known_data = curr_and_past_data.select(self.name).to_numpy()
        self.known_data = np.flip(self.known_data)

    def __getitem__(self, offset: int) -> float:
        if self.known_data is None:
            raise ValueError("No data available. Call `getKnownData` first.")
        return self.known_data[offset]

def onrow(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        equities = {eq.name: eq for eq in self.equities}
        namespace = SimpleNamespace(**equities)
        return func(self, *args, namespace, **kwargs)
    return wrapper

@dataclass
class Row():
    open: float
    high: float
    low: float
    close: float
    volume:float
