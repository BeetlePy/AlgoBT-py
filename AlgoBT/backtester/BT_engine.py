import polars as pl
from typing import Optional
import numpy as np
from BT_utils import Equity, Indicator, onrow
from orders import OrderSim, Order, OrderBook
from contextlib import contextmanager
from functools import wraps
from abc import ABC, abstractmethod


class BTest(ABC):
    """Parent class for backtesting engine.
    Required methods:
    __init___(),,
    onRow()"""

    def __init__(self):
        pass

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        original_init = cls.__init__

        def wrapped_init(self, *args, **kwargs):
            self.equities = []
            original_init(self, *args, **kwargs)
            self.__postinit__()

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

    @abstractmethod
    def onRow(self):
        pass
    
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
            for eq in self.equities:
                eq.updateIndicators(self.current_timestamp)
            self.onRow()

    def initEquity(self, ticker: str, data: pl.DataFrame, timeframe: str, name:str) -> Equity:
        """Creates an Equity class and stores data

        :param ticker: Ticker of equtiy
        :param data: DataFrame of OHLC(V) data
        :param timeframe: timeframe of data. Lowest timeframe of all timeframes to be added per this equity.
        :param name: name of equtiy class instance.
        """
        eq = Equity(ticker=ticker, bt_object=self, timeframe=timeframe, name=name)
        self.__addDefaultIndicators(data, eq)  # Add OHLCV as indicators.
        self.equities.append(eq)
        return eq

    def addIndicator(self, df, equity_object: Equity, name, col_name, calc_function=None):
        """Adds an Indicator to an existing equity object.

        "param df: Datframe with indicator value or values for indicator calculation source.
        :param equity_object: Existing Equtiy() instance to add indicator.
        :param alias: Name of indicator to be added. Will become Equity() atrribuite.
        :param col_name: Name of column to be used for indicator.
        :param calc_function: Function used to calculate indicator. Optional if column already exists in indicator.
        """
        equity_object.addIndicator(df, col_name, name)

    def __addDefaultIndicators(self, df, equity_object: Equity):
        self.addIndicator(df, equity_object, "open", "open")
        self.addIndicator(df, equity_object, "high", "high")
        self.addIndicator(df, equity_object, "low", "low")
        self.addIndicator(df, equity_object, "close", "close")
        self.addIndicator(df, equity_object, "volume", "volume")

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
