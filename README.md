# AlgoBT-py
Framework for algorithmic Trading. Under heavy development, not currently functional.

#BTest
**Warning: Btest is not complete nor functional.**
BT test is an event driven backtesting framework, designed for ease of using multiple timeframes, among multiple equities. It is built on polars and numpy for maximum speed.

## Adding equities and data

To add an equity to your strategy, use self.initEquity():
You'll need to pass a pl.DataFrame, containing the following columns and types.
- timestamp: pl.Datetime
- open: float
- high: float
- low: float
- close: float
- volume: int
  
For example, to add SPY to your strategy:
```
YourStrategy(BTest)
    def __init__(self):
        self.initEqutiy("SPY", spy_df, timeframe="1m", name="SPY")
```
This initializes an Equity() class, which allows you to fetch data.
Next, we will add an indicator to our spy equity, however first we must calculate our indicator. There is no built in indicators at the moment, so we will have to do it on our own.

`spy_df = spy_df.with_columns(pl.col("close").rolling_mean(20).alias("sma20"))`

To add an indicator, simply use self.addIndicator().
```
YourStrategy(BTest)
    def __init__(self):
        self.spy = self.initEqutiy("SPY", spy_df, timeframe="1m", name="SPY")
        self.addIndicator(spy_df, equity_object=self.spy, name="sma20", col_name="sma20)
```
## Accesing indicators and OHLCV
In order to acces any indicators, you must use the onRow() method with the @onrow decorator. In addition to self, you must pass a parameter. It will become the refrence for the data accessor. It is reccomended to use d as the parameter.
```
@onrow
def onRow(self, d):
    close = d.spy.close[0]
    sma20 = d.spy.sma20[0]
```
To access an indicator use the folling format:
d.equity.indicator[index]
The index value is equivilant to how many bars **back** in time you want to acces. Ex: d.equity.indicator[0] is the current value, and d.equity.indicator[1] is the previous.
Open, high, low, close, and volume are automaticly created as indicators when an equity is created, and behave the same.

