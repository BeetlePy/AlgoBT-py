from imports import *
from dataclasses import dataclass, field
from typing import Optional, Literal
from sortedcontainers import SortedList


oid = 0  # Order ID


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
        self.open_stops = []
        # Lists of orders that trigger when price is greater/less than trigger price
        # All limit and stop orders should be appended to the list.
        self.trigger_gt_orders = SortedList(key=lambda Order: Order.price_placed)
        self.trigger_lt_orders = SortedList(key=lambda Order: Order.price_placed)
        
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
        if self.long_signal:
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
        for row in self.data_df.iter_rows():
            # REMEMBER, ROW IS A TUPLE!!!!!
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
            self.acount.updateAssetValue(self.symbol, timestamp=self.time, price=self.close)
            self.onRow()  # User-Implemented behaviour
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
            return
        elif direction == 2:
            self.trigger_gt_orders.append(order)
            self.trigger_gt_orders.addTrade(order)
        
    def _checkOpenOrders(self, open, high, low, close):
        for order in self.trigger_gt_orders:
            if order.price_placed == high and order.order_type == "stop":
                updated_order = self._simOrderFill(time=self.time, price=order.price_placed,qty=order.qty,
                                  existing_order=order, commision_per=order.future_commision)
                TradesLedger.updateTrade(updated_order)
            elif order.price_placed > high:
                pass
        
                        
            
            
class Metrics():
    """A class to manage and display backtest preformance metrics,
        acount balance metrics, and rolling stats (rolling win % ect.)"""
    def __init__(self, data_df):
        pass
        
        
@dataclass
class Acount():
    cash_balance: float
    assets_value: float

    def __post_init__(self):
        self.total_value = self.cash_balance + self.assets_value
        self.assets_book = []
        self.acount_df = pd.DataFrame
        # ToDo: Once mulit-asset support is added, use a list comp to init the assets book.
        self.assets_book.append({"symbol": self.symbol, "price": None, "qty": 0})

    def setAssets(self, asset_symbol, price, qty):
        # Get the dictionary of asset from self.assets_book.
        asset_dict = [d for d in self.assets_book if d["symbol"] == asset_symbol][0]
        asset_dict['qty'] += qty
        self.cash_balance -= qty * price
        
    def removeAssets(self, asset_symbol, qty) -> None:
        """
        removes a specified amount of assets from the asset book.
        If it amounts to all the 

        :param asset_symbol: symbol of asset to remove
        :param qty: quantity of asset to remove
        """
        temp_assets_book = self.assets_book
        for index, asset in enumerate(temp_assets_book):
            if asset["symbol"] == asset_symbol:
                if asset["qty"] == qty:
                    self.assets_book.remove(asset)
                    return
                elif asset["qty"] < qty:
                    raise ValueError("Error: Cannot remove more assets than held")
                asset["qty"] = qty
                self.assets_book[index] = asset
                return
        
    def logAcount(self, timestamp):
        self.acount_df.at[timestamp, "cash_balance"] = self.cash_balance
        self.acount_df.at[timestamp, "acount_value"] = self.cash_balance

    def getTotalAssetValue(self):
        # Intended to be called when assets values are fully updated.
        # sets self.assets_value to total value of all assets
        values = []
        for a in self.assets_book:
            price = a["price"]
            qty = a["qty"]
            value = price * qty
            values.append(value)
        self.assets_value = sum(values)
        self.total_value = self.assets_value + self.cash_balance

    def updateAssetValue(self, symbol: str, timestamp: pl.Datetime, price: float):
        for asset in self.assets_book:
            if asset == symbol:
                asset["price"] = price
                break  # we don't need any other info


@dataclass
class Order():
    """_summary_
    Class containing metadata about an order.
    """
    # Represents a single trading order with relevant metadata.
    status: Literal["open", "filled", "canceled"]  # Whether the order was canceled.
    time_placed: dt  # Time when the order was placed.
    price_placed: float  # Price at which the order was placed.
    qty: float  # Quantity of shares in the transaction.
    direction: Literal[1, 2]  # 1 = buy, 2 = sell
    order_type: Optional[Literal["Market", "Limit", "Stop", "Trailing-Stop"]]  # Type of order.
    fees_paid: Optional[float] = 0  # Fees associated with the order.
    short: Optional[float] = False 
    # If the order will operate on borrowed assets or not. A buy order with the short param,
    # will be a cover.
    price_filled: Optional[float] = None  # Price at which the order was filled (if any).
    usd_cost: Optional[float] = None  # Total cost of the order in USD, including fees.
    order_reason: Optional[str] = None  # Notes on the order (e.g., "Exit condition, RSI > 70").
    future_commision: Optional[str] = None

    def __post_init__(self):
        if self.direction not in (1, 2):
            raise ValueError("`direction` must be 1 (buy) or 2 (sell).")
        self.order_id = _generateOrderId()   # A unique order id

    def _generateOrderId(self):
        return self.last_id + 1
    
    
@dataclass
class TradesLedger():
    # Represents a collection of trades, organized into parent and daughter orders.
    orderbook: list[int, Order]  # [id: Order, id: Order]
    last_id: int = 0
    
    def getTradeById(self, trade_id) -> dict:
        return self.orderbook[trade_id]
        
    def addTrade(self, order: Order) -> None:
        # Add a trade to the ledger.
        # param order: The order.
        self.orderbook.append({order.order_id, order})

    def updateTrade(self, order: Order) -> None:
        id = order.order_id
        # Keep all orders not except for the one with the pass arg: id
        self.orderbook.append([x for x in self.orderbook if x['id'] != id])
        # Add the updated order back to the list
        self.orderbook.append(order)

    def filterTrades(self, reason: str) -> list[dict[str, Order]]:
        # Filter trades based on a specific order reason.
        # param reason: A substring to match in the `order_reason`.
        # return: A list of trades matching the filter criteria.
        return [
            trade for trade in self.orderbook
            if trade["parentOrder"].order_reason and reason in trade["parentOrder"].order_reason
        ]