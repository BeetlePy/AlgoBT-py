import polars as pl
from typing import Optional
import numpy as np
from BT_utils import Equity, Indicator, onrow, Duration
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
        self._handlers = {}
        self.__registerHandlers()

        if not hasattr(self, "cash"):
            print("Cash balance not set. Defaulting to 100k.")
            self.cash = 100000

        if not hasattr(self, "commision_per"):
            print("Comision percent not set. Defaulting to 0.05%")
            self.commision_per = 0.0005
    
    def __createTimeline(self) -> list:
        timeline = []
        for eq in self.equities:
            timeline.extend(list(zip(eq.df["timestamp"], [eq.timeframe] * len(eq.df))))
        timeline = sorted(set(timeline), key=lambda x: x[0])  # Convert to set and sort by first value.
        return timeline

    @abstractmethod
    def onRow(self, data_alias, timeframe: Duration):
        pass

    def run(self):
        timeline = self.__createTimeline()
        for timestamp, timeframe in timeline:
            self.current_timestamp = timestamp
            for eq in self.equities:
                eq.updateIndicators(self.current_timestamp)
            self.__triggerHandler(timeframe=timeframe)

    def initEquity(self, ticker: str, data: pl.DataFrame, timeframe: str, name:str) -> Equity:
        """Creates an Equity class and stores data

        :param ticker: Ticker of equtiy
        :param data: DataFrame of OHLC(V) data
        :param timeframe: timeframe of data. Lowest timeframe of all timeframes to be added per this equity.
        :param name: name of equtiy class instance.
        """
        eq = Equity(df=data, ticker=ticker, bt_object=self, timeframe=timeframe, name=name)
        self.__addDefaultIndicators(data, eq)  # Add OHLCV as indicators.
        self.equities.append(eq)
        return eq

    def addIndicator(self, df, equity_object: Equity, name, col_name, calc_function=None):
        """Adds an Indicator to an existing equity object.

        "param df: Datframe with indicator value or values for indicator calculation source.
        :param equity_object: Existing Equtiy() instance to add indicator.
        :param alias: Name of indicator to be added. Will become Equity() atrribuite.
        :param col_name: Name of column to be used for indicator.
        :param calc_function: Function used to calculate indicator. Optional if indicator alread is a column.
        """
        if calc_function is not None:
            df = calc_function(equity_object.df)
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

    def __registerHandlers(self):
        # Automatically register methods decorated with @onrow
        for method_name in dir(self):  # Iterate through all methods in self.
            method = getattr(self, method_name)  # Get the method.
            if hasattr(method, "_onrow_params"):  # If _onrow_params were passed into the method (timeframe)
                params = method._onrow_params  # Dictionary of params
                timeframe = params.get("timeframe", "default")
                self._handlers[timeframe] = method

    def __triggerHandler(self, timeframe="default", data=None):
        handler = self._handlers.get(timeframe)
        if handler:
            handler(data)
        else:
            raise ValueError(f"No handler for timeframe {timeframe}")
