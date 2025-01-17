from dataclasses import dataclass
from typing import Optional, Literal
from imports import *

class Metrics:
    """A class to manage and display backtest preformance metrics,
    acount balance metrics, and rolling stats (rolling win % ect.)"""

    def __init__(self, data_df):
        pass


@dataclass
class Acount:
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
        asset_dict["qty"] += qty
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
class Order:
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
        self.order_id = _generateOrderId()  # A unique order id


@dataclass
class TradesLedger:
    # Represents a collection of trades, organized into parent and daughter orders.
    orderbook: list[dict[int, Order]]

    def getTradeById(self, trade_id) -> dict:
        return self.orderbook[trade_id]

    def addTrade(self, order: Order) -> None:
        # Add a trade to the ledger.
        # param order: The order.
        if order.order_id < self.last_id:
            # If order id has been used before
            self.orderbook = [x for x in self.orderbook if x["id"] != order.order_id]

        self.orderbook.append({order.order_id, order})

    def updateTrade(self, order: Order) -> None:
        oid = order.order_id
        # Keep all orders not except for the one with the pass arg: id
        self.orderbook = [x for x in self.orderbook if x["id"] != oid]
        # Add the updated order back to the list
        self.orderbook.append(order)

    def filterTrades(self, reason: str) -> list[dict[str, Order]]:
        # Filter trades based on a specific order reason.
        # param reason: A substring to match in the `order_reason`.
        # return: A list of trades matching the filter criteria.
        return [
            trade
            for trade in self.orderbook
            if trade["parentOrder"].order_reason and reason in trade["parentOrder"].order_reason
        ]


last_id = 0


def _generateOrderId():
    global last_id
    last_id += 1
    return last_id
