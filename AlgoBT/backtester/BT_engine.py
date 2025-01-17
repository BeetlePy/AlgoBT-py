from imports import *
from typing import Optional
import numpy as np
from BT_utils import Acount, Order, Metrics, TradesLedger


class BTest():
    """ Parent class for backtesting engine.
        Required methods:
        __init___(),
        setSignals(),
        onRow()"""
    def __init__(self):
        # data_dfs is a dictionary; keys are tickers; values are Polars DataFrames (same ticker)
        # all needed collums/data needs to have been generated.
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
        self.acount = Acount(self.starting_cash, 0)  # Initialize the acount tracking class
        self.trades_ledger = TradesLedger()
        if self.commision_per not in globals() or self.commision_per not in locals():
            self.commision_per = 0.0015  # defualt
        if self.slip_range not in globals() or self.slip_range not in locals():
            self.slip_range = (-0.0005, 0.0005)  # default
        # Arrayss of orders that trigger when price is greater/less than trigger price
        # All limit and stop orders should be appended to the array.
        self.trigger_gt_orders = np.array([])
        self.trigger_lt_orders = np.array([])
        
    def setSignals(self):
        raise NotImplementedError("This method should be overridden in subclasses")
    
    def onRow(self):
        raise NotImplementedError("This method should be overridden in subclasses")
    
    def run(self):
        """DO NOT OVERWRITE!"""
        timestamp_idx, open_idx, close_idx, high_idx, low_idx, lsingal_idx, ssignal_idx = self._runInit()
        self._runLoop(timestamp_idx, open_idx, close_idx, high_idx, low_idx, lsingal_idx, ssignal_idx)  # ):<

    def _runInit(self):
        self.data_df = self.data_df.drop_nulls()
        # Initialize acount traking df and store it in Acount class
        acdf = self.data_df.with_columns([pl.col("timestamp")]).to_pandas()
        acdf["acount_value"] = np.nan
        acdf["cash_balance"] = np.nan
        self.acount.acount_df = acdf

        if self.long_signal:  # If strategy inclueds longsignals
            long_signal_idx = self.data_df.get_column_index("long_signal")
        else:
            long_signal_idx = None
        if self.short_signal:
            short_signal_idx = self.data_df.get_column_index("short_signal")
        else:
            short_signal_idx = None
        timestamp_idx = self.data_df.get_column_index("timestamp")
        open_idx = self.data_df.get_column_index("open")
        close_idx = self.data_df.get_column_index("close")
        high_idx = self.data_df.get_column_index("high")
        low_idx = self.data_df.get_column_index("low")
        return timestamp_idx, open_idx, close_idx, high_idx, low_idx, long_signal_idx, short_signal_idx

    def _runLoop(self, timestamp_idx, open_idx, close_idx, high_idx, low_idx, long_signal_idx, short_signal_idx):
        for row in self.data_df.to_numpy():
            # Df is np array
            # With this engine, there is no way to know if the high/low was first, and when they happned.
            # Due to this we have to make some assumptions.
            # if the close is > open, then the low happend first, and vice versa
            # In addition, we have no way of knowing when a price triggerd the stop. So, it will happen at the end,
            # for no chance of using money we would not have had
            self.time = row[timestamp_idx]
            self.close = row[close_idx]
            self.open = row[open_idx]
            self.long_signal = row[long_signal_idx]
            self.short_signal = row[short_signal_idx]
            self.onRow()  # User-Implemented behaviour
            self._checkOpenOrders(self.open, self.high, self.low, self.close, self.time)
            self.acount.updateAssetValue(self.symbol, timestamp=self.time, price=self.close)
            self.acount.logAcount(self.time)  # Log the updated acount attribuites to the acount df

    def marketOrder(self, qty, commision_per: Optional[float] = None,
                    slip_range: Optional[tuple] = None, reason: Optional[str] = ''):
        """(:<
        Returns an instance of the order class based on provided params"""

        price = self.price
        timestamp = self.time
        order = Order(canceled=False, time_placed=timestamp, price_placed=price,
                      usd_cost=cost,
                      order_reason=reason)
        self._simOrderFill(self, order)
        return

    def _simOrderFill(self, time, price, qty, existing_order: Order, commision_per: Optional[float]=None, 
                      slip_range: Optional[float]=None) -> Order:
        price = self.price
        if commision_per is None:
            commision_per = self.commision_per
        if slip_range is None:
            slip_range = self.slip_range
        price_filled = price * 1 + np.random.randint(slip_range)  # Slippage
        fees = price_filled * (1 + commision_per)
        cost = fees + price_filled
        if cost > self.acount.cash_balance and existing_order.direction == 1 and not existing_order.short:
            existing_order.status = "canceled"
            return order
        if existing_order is not None:
            existing_order.price_filled = price_filled
        self.acount.setAssets(self.symbol, price_filled, qty)
        self.trades_ledger.addTrade(order)
        return order

    def limitOrder(self, price, qty, commision_per: Optional[float] = None,
                slip_range: Optional[tuple] = None, reason: Optional[str] = ''):
        """
        (:
        Places a limit order, then adds it the trigger_gt_orders or trigger_lt_orders SortedLists.
        The list it is added to corresponds with the order qty, signaling direction.

        :param price: price to sumbit order at
        :param qty: amount of instrument to be purchased. A buy is a positve qty, sell is negative
        :param commision_per: commision percent. Defaults to None.
        :param slip_range: range to use for random slippage. Defaults to None.
        :param reason: order reason. Defaults to ''.
        """

        if qty > 0:
            direction = 1
        elif qty < 0:
            direction = 2
        else:
            raise ValueError("param qty invalid: qty cannot be zero! ): ")
        order = Order(status="open", time_placed=time, qty=qty,
                     direction=direction, order_type="limit", price_placed=price)
        if direction == 1:  # Long
            self.trigger_lt_orders.append(order)
            self.trades_ledger.addTrade(order)

            return
        else:
            self.trigger_gt_orders.append(order)
            self.trades_ledger.addTrade(order)
            return
        
    
    def stopOrder(self, time: pl.Datetime, qty: float, price, commision_per: Optional[float] = None):
        """
        :param time: pl.datetime when the order is placed.
        :param qty: Position size: Negaitive means a sell order.
        :param price: Price to place the order at.
        """
        if qty > 0:
            direction = 1
        elif qty < 0:
            direction = 2
        else:
            raise ValueError("param qty invalid: qty cannot be zero! ): ")
        order = Order(status="open", time_placed=time, qty=qty,
                     direction=direction, order_type="Stop", price_filled=price, future_commision=commision_per)
        if direction == 1:
            self.trigger_lt_orders.append(order)
            self.trades_ledger.addTrade(order)
        elif direction == 2:
            self.trigger_gt_orders.append(order)
            self.trigger_gt_orders.addTrade(order)
        
    def _checkOpenOrders(self, open, high, low, close, time):
        triggered_gt_orders = self.trigger_gt_orders[self.trigger_gt_orders < high]
        triggered_lt_orders = self.trigger_lt_orders[self.trigger_lt_orders > low]
        for order in triggered_gt_orders:
            updated_order = self._simOrderFill(time, order.price_placed, order.qty, order)
            TradesLedger.updateTrade(updated_order)
        for order in triggered_lt_orders:
            updated_order = self._simOrderFill(time, order.price_placed, order.qty, order)
            TradesLedger.updateTrade(updated_order)
