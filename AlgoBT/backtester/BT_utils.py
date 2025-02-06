from dataclasses import dataclass
from typing import Optional, Literal
from imports import *
from functools import wraps
from types import SimpleNamespace


class Equity:
    __slots__ = ("indicators", "bt_object", "ticker", "timeframe", "name", "df")

    def __init__(self, df: pl.DataFrame, ticker: str, bt_object: object, timeframe, name):
        self.df = df
        self.name = name
        self.timeframe = timeframe
        self.ticker = ticker
        self.bt_object = bt_object
        self.indicators = {}  # Dict of indicators. Includes OHLCV.

    def updateIndicators(self, cur_timestamp: pl.Datetime) -> None:
        for _, ind in self.indicators.items():
            ind.getKnownData(cur_timestamp)

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

def onrow(*decorator_args, **decorator_kwargs):
    def decorator(func):
        func._onrow_params = decorator_kwargs

        @wraps(func)
        def wrapper(self, *args, **kwargs):
            equities = {eq.name: eq for eq in self.equities}
            namespace = SimpleNamespace(**equities)
            return func(self, namespace, **kwargs)

        return wrapper

    # Handle both @onrow and @onrow(timeframe="1m")
    if decorator_args and callable(decorator_args[0]):
        return decorator(decorator_args[0])
    else:
        return decorator


class Duration:
    def __init__(self, str_format=None, polars_format=None):
        if str_format is not None:
            self.str_format = str_format
            if polars_format is None:
                self.polars_format = self.stringToPolars(self.str_format)
        if polars_format is not None:
            self.polars_format = polars_format
            if str_format is None:
                self.str_format = self.polarsToString(self.polars_format)
        if str_format is None and polars_format is None:
            raise ValueError("At least one string or polars format must be passed.")

    def stringToPolars(self, str_):
        # s, m, h, d
        str_ = list(str_)
        time_unit = [s for s in str_ if isinstance(s, str)]
        unit_amount = [s for s in str_ if isinstance(s, int)]
        match time_unit:
            case "s":
                return pl.duration(seconds=unit_amount)
            case "m":
                return pl.duration(minutes=unit_amount)
            case "h":
                return pl.duration(hours=unit_amount)
            case "d":
                return pl.duration(days=unit_amount)
        raise ValueError("Error: Invalid string format.")
    
    def polarsToString(self, dur):
        dur = list(dur)
        ints = [str(i) for i in dur if isinstance(i, int)]
        unit_amount = ''.join(ints)
        time_unit = [x for x in dur if isinstance(x, str)][0]
        _str = unit_amount + time_unit
        return _str


@dataclass(slots=True)
class Row:
    timestamp: pl.Datetime
    timeframe: Duration
    open: float
    high: float
    low: float
    close: float
    volume:float
    equtiy: Equity
