import polars as pl
from typing import Optional
import numpy as np
from backtester_utils import Equity, Indicator


class BTest():
    """Parent class for backtesting engine.
    Required methods:
    __init___(),,
    onRow()"""

    def __init__(self):
        raise NotImplementedError("This method should be overridden in subclasses")

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Store the original __init__ of the subclass
        original_init = cls.__init__

        def wrapped_init(self, *args, **kwargs):
            # Call the original __init__
            original_init(self, *args, **kwargs)
            # Call __postinit__ after the subclass's __init__
            self.__postinit__()

        # Replace the subclass's __init__ with the wrapped version
        cls.__init__ = wrapped_init

    def __postinit__(self):
        try:
            _ = self.cash
        except NameError:
            print("Cash balance not set. Defaulting to 100k.")


    def onRow(self):
        raise NotImplementedError("This method should be overridden in subclasses")

    def run(self):
        self.orderCols()  # ToDo: Create function to properly order columns into the correct order
        for row in self.master_df.iter_rows():
            self.current_timestamp = row[0]
            self.onRow()

    def initEquity(self, ticker: str, data: pl.DataFrame, timeframe: str) -> Equity:
        """Creates an Equity class and stores data

        :param ticker: Ticker of equtiy
        :param data: DataFrame of OHLC(V) data
        :param timeframe: timeframe of data. Lowest timeframe of all timeframes to be added per this equity.
        """
        eq = Equity(ticker=ticker, timeframe=timeframe, df=data)
        return eq
    
    def initIndicator(self, equity, calc, timeframe, window_length=1, name: Optional[str] = "") -> Indicator:
        df = getattr(equity, f"{timeframe}_df")
        df = calc(df)
        if name == "":
            name = f"indicator{len(df.columns)}"
        ind = Indicator(df=df, col=name, bt_class=self)
        return ind


    def marketOrder():
        return

    def _simOrderFill():
        return

    def limitOrder():
        return

    def stopOrder():
        pass
    
    def _checkOpenOrders(self, open, high, low, close, time):
        pass
