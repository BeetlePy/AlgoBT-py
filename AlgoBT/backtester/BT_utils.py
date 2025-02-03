from dataclasses import dataclass
from typing import Optional, Literal
from imports import *


@dataclass
class Equity():
    ticker: str  # Ticker object for equty

    def __post_init__(self):
        self.timeframes = {}
    
    def addTimeframe(self, data_df: pl.DataFrame, timeframe: str):
        self.timeframes[timeframe] = data_df

@dataclass
class Indicator():
    df: pl.DataFrame  # Dataframe where the column containing the indicator resides.
    col: str  # Name of col in self.df
    bt_class: type[object]

    def __getitem__(self, index_offset: int):
        timestamp_col = self.df["timestamp"].cast(pl.Datetime("ns")).to_numpy()
        target_ts = np.datetime64(self.bt_class.current_timestamp)
        matches = np.where(timestamp_col == target_ts)[0]
        if not matches.size:
            raise KeyError(f"No data for timestamp {target_ts}")
        base_row = matches[0]
        requested_row = base_row - index_offset

        if requested_row < 0 or requested_row >= len(self.df):
            raise IndexError(f"Offset {index_offset} out of bounds")

        return self.df.row(requested_row, named=True)

def onrow(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.use_attributes() as (open, close, high, low, volume):
            # Inject attributes into the function's scope
            return func(self, open, close, high, low, volume, *args, **kwargs)

    return wrapper
@dataclass
class Row():
    open: float
    high: float
    low: float
    close: float
    volume:float
