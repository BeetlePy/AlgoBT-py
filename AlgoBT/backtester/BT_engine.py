import polars as pl
from typing import Optional
import numpy as np
from backtester_utils import Equity, Indicator, onrow
from orders import OrderSim, Order, OrderBook
from contextlib import contextmanager
from functools import wraps


class BTest():
    """Parent class for backtesting engine.
    Required methods:
    __init___(),,
    onRow()"""

    __slots__ = ("open", "close", "high", "low", "volume")  # OHLCV

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
        self.orderSim = OrderSim()
        self.orderBook = OrderBook([])
        try:
            _ = self.cash
        except AttributeError:
            print("Cash balance not set. Defaulting to 100k.")
            self.cash = 100000

        try:
            _ = self.commision_per
        except AttributeError:
            print("Comision percent not set. Defaulting to 0.05%")
            self.commision_per = 0.0005

    def onRow(self):
        raise NotImplementedError("This method should be overridden in subclasses")
    
    def orderCols(self):
        sorted_cols = ["timestamp", "open", "high", "low", "close", "volume", "avg_volume"]
        remaining_cols = [col for col in self.master_df.columns if col not in sorted_cols]
        self.master_df = self.master_df.select(sorted_cols + remaining_cols)
    


    def run(self):
        self.master_df = self.master_df.with_columns(pl.col("volume").rolling_mean(100).fill_null(pl.col("volume"))
                                                    .alias("avg_volume"))
        self.orderCols()
        for row in self.master_df.iter_rows():
            self.current_timestamp = row[0]
            self.close = row[4]
            self.open = row[1]
            self.high = row[2]
            self.low = row[3]
            self.volume = row[5]
            self.avg_volume = row[6]
            self.onRow()

    def initEquity(self, ticker: str, data: pl.DataFrame, timeframe: str) -> Equity:
        """Creates an Equity class and stores data

        :param ticker: Ticker of equtiy
        :param data: DataFrame of OHLC(V) data
        :param timeframe: timeframe of data. Lowest timeframe of all timeframes to be added per this equity.
        """
        eq = Equity(ticker=ticker)
        eq.addTimeframe(data_df=data, timeframe=timeframe)
        return eq
    
    def initIndicator(self, equity, calc, timeframe, name: Optional[str] = "") -> Indicator:
        df = getattr(equity, f"{timeframe}_df")
        df = calc(df)
        if name == "":
            name = f"indicator{len(df.columns)}"
        ind = Indicator(df=df, col=name, bt_class=self)
        return ind


    def marketOrder(self, equity, qty: float, order_side: str):
        order, cost = self.orderSim.createMarketOrder(price=open, qty=qty, time=self.current_timestamp,
                                                    side=order_side, open=self.open, high=self.high, low=self.low,
                                                    close=self.close, volume=self.volume, avg_volume=self.avg_volume)
        self.orderBook.addOrder(order)
        if order_side in ("BUY", "COVER"):
            self.cash -= cost
            self.cash -= cost * self.commision_per
        elif order_side in ("SELL", "SHORT"):
            self.cash += cost
            self.cash -= cost * self.commision_per
        print(f"{order_side} {qty} {equity.ticker} at {order.price_filled} for {cost}")

    def limitOrder():
        return

    def stopOrder():
        pass
    
    @contextmanager
    def use_attributes(self):
        yield self.open, self.close, self.high, self.low, self.volume
