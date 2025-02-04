from dataclasses import dataclass
from typing import Optional, Literal
from imports import *


class Equity:
    __slots__ = ("_current_idx", "indicaotrs", "bt_object", "ticker", "timeframe")

    def __init__(self, ticker: str, bt_object: object, current_idx: int, timeframe):
        self.timeframe = timeframe
        self.ticker = ticker
        self.bt_object = bt_object
        self.latest_idx = current_idx
        self.indicators = {}  # Dict of indicators. Includes OHLCV.

    def updateIndicators(self, cur_timestamp: pl.Datetime) -> None:
        for _, ind in self.indicators.items():
            ind.getKnownData(cur_timestamp)

    def addIndicator(self, col_name, name):
        """Usage:\n
        Equity.name[index]\n
        Ex: SPY.close[1]\n

        :param col_name: name of the indicator column indide the column.
        :param name: name of indicator for refrence.
        """
        indicator = Indicator(df.select("timestamp", "col"), self.timeframe)
        self.indicators[name] = indicator

    def __getattribute__(self, name: str):
        """Dot notation access: equity.close"""
        if name in self.indicators.keys():
            return self.indicators[name]
        else:
            raise AttributeError(f"Error: name: {name} not in self.indicators.")

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
        with self.use_attributes() as (open, close, high, low, volume):
            # Inject attributes into the function's scope
            return func(self, open, close, high, low, volume, *args, **kwargs)

    return wrapper

@dataclass
class Row():
    open: float
    high: float
    low: float
    close: float
    volume:float
