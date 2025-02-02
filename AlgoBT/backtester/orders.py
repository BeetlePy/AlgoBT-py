import polars as pl
from dataclasses import dataclass
from typing import Literal, Optional
from sortedcontainers import SortedList


class OrderSim():
    def __init__(self):
        self.lastOID = 0

    def createMarketOrder(self, price, qty, time, side, open, high, low, close, volume, avg_volume):
        """creates and simulates filling of a market order.

        :param price: price to attempt to fill order at.
        :param qty: Amount of shares to use in order
        :param time: time of order placement
        :param side: order side. One of the following: "BUY", "SELL", "SHORT", "COVER"
        :param open: open price of current bar
        :param high: high price of current bar
        :param low: low price of current bar
        :param close: close price of current bar
        :param volume: volume of current bar
        :param avg_volume: average volume of past 100 bars
        :return: returns the order odject, and the cost of filling the order.
        """
        price_filled = self.simOrderFill(price, open, high, low, close, volume, avg_volume, side)
        order = Order(oid=self.genOrderID, type_="MARKET", side=side, time_placed=time,
                     time_filled=time, price_filled=price_filled)
        cost = qty * price_filled
        return order, cost

    def simOrderFill(self, price_to_fill, open, high, low, close, volume, avg_volume, order_side, qty):
        upper_ratio, lower_ratio = calculateWickRatios(open, high, low, close)
        slippage = self.calculateSlippage(upper_ratio, lower_ratio, volume, avg_volume)
        if qty > volume * 0.2:
            price_to_fill = (open + close) / 2  # Use mid price if order is learge relative to volume.

        if order_side in ("BUY", "COVER"):
            price_filled = price_to_fill * (1 + slippage)
        else:
            price_filled = price_to_fill * (1 - slippage)
        return price_filled


    def calculateSlippage(upper_ratio, lower_ratio, volume, avg_volume):
        # Base slippage (bps)
        base_slip = 2.0
        # Adjust for wicks (toxic flow)
        if upper_ratio > 1.5 or lower_ratio > 1.5:
            base_slip *= 2.0  # Double slippage for large wicks
        # Adjust for volume
        volume_ratio = volume / avg_volume
        if volume_ratio < 0.5:
            base_slip *= 1.5  # Wider spreads in low volume
        elif volume_ratio > 2.0:
            base_slip *= 1.2  # More competition in high volume
        
        return base_slip / 10_000  # Convert to decimal

    def genOrderID(self):
        self.lastOID += 1
        return self.lastOID

def calculateWickRatios(open, high, low, close):
    upper_wick = high - max(open, close)
    lower_wick = min(open, close) - low
    body_size = abs(close - open)

    upper_ratio = upper_wick / body_size if body_size > 0 else 0
    lower_ratio = lower_wick / body_size if body_size > 0 else 0

    return upper_ratio, lower_ratio

@dataclass
class Order():
    oid: int
    qty: float
    type_: Literal["LIMIT", "MARKET", "STOP"]
    side: Literal["BUY", "SELL", "SHORT", "COVER"]
    time_placed: pl.Datetime
    price_placed: float
    status: Literal["OPEN", "FILLED", "CANCELED"]
    price_filled: Optional[float]
    time_filled: Optional[pl.Datetime]

    def __lt__(self, other):
        return self.price_placed < other.price_placed


@dataclass
class OrderBook():
    all_orders: list

    def __post_init__(self):
        self.open_orders = SortedList()

    def addOrder(self, order):
        if order.status == "OPEN":
            self.open_orders.add(order)
        self.all_orders.append(order)

    def removeOpenOrder(self, order):
        if order in self.open_orders:
            self.open_orders.remove(order)


