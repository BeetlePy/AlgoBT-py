from dataclasses import dataclass
from typing import Optional, Literal
from imports import *


@dataclass
class Equity():
    ticker: str  # Ticker object for equty
    timeframe: str  # Lowest timeframe
    df: pl.DataFrame  # Base df

    def __post_init__(self):
        setattr(self, f"{self.timeframe}_df", self.df)
    
    def addTimeframe(self, data_df, timeframe):
        setattr(self, f"{timeframe}_df", data_df)

@dataclass
class Indicator():
    df: pl.DataFrame  # Dataframe where the column coanting the indicator resides.
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
