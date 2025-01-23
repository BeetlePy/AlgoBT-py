import numpy as np
import scipy as sp
import polars as pl
import hyppo as hp
import future
import matplotlib.pyplot as plt
from typing import Literal

class Research():
    def __init__(self) -> None:
        pass

    def loadFileTypes(self, path: str, type: Literal["csv", "parquet", "json"]) -> pl.DataFrame:
        if type == "csv":
            return pl.read_csv(path)
        elif type == "parquet":
            return pl.read_parquet(path)
        elif type == "json":
            return pl.read_json(path)
        else:
            raise ValueError("param: type must be a valid file type. Please use csv, parquet, or json.")
        
class ResearchStrat():
    """A simple, vectorized backtesting framework to quickly write out, test and evaluate new ideas."""

    def __init__(self):
        raise NotImplementedError("__init__() must be overriden in child class")

    def __postinit__(self):
        self.dfs = {}
        self._LoadData()
    
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

    def _LoadData(self):
        # Check if 'self.files' is defined and is a dictionary.
        if not hasattr(self, "files") or not isinstance(self.files, dict):
            raise TypeError(
                "Attribute 'self.files' must be a dictionary filled with the symbols and file paths.")
        for symbol, path in self.files.items():
            path_list = list(path)
            # Parses the path str to obtain the file type at the end of the file name.abs
            try:
                file_type = ''.join(path_list[len(path_list) - 1 - path_list[::-1].index('.') + 1:])
            except Exception:
                raise ValueError(
                    f"ERROR: file type: {file_type} in file path: {path} is invalid. "
                    "Only files with the suffixes .csv, .parquet, and .json."
                )

            try:
                df = Research().loadFileTypes(path=path, type=file_type)  # Attempt to load the dataframe
                df = df.with_columns(((pl.col("close").shift(-1) - pl.col("close")) / pl.col("close")).alias("returns"))
                self.dfs[symbol] = df
            except Exception as e:
                raise ValueError(f"Error loading parquet file for symbol '{symbol}' at path '{path}': {e}")
        
        # Initialize the universe attr as a list of asset symbols.
        self.universe = list(self.files.keys())
            
    def trackIsInTrade(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Track whether the strategy is currently in a trade (long or short).

        :param df: Polars DataFrame with required columns:
                ['long_entry', 'long_exit', 'short_entry', 'short_exit'].
        :return: DataFrame with 'is_long' and 'is_short' columns indicating active trades.
        """
        required_columns = {"long_entry", "long_exit", "short_entry", "short_exit"}
        if missing := required_columns - set(df.columns):
            raise ValueError(f"Missing columns: {missing}")

        for direction in ["long", "short"]:
            entry_col = f"{direction}_entry"
            exit_col = f"{direction}_exit"
            position_col = f"is_{direction}"

            # Initialize state column
            df = df.with_columns(pl.lit(0).alias("_state"))

            # Use a custom function to handle state transitions
            def state_transition(entry, exit, state):
                if entry == 1 and state == 0:  # Enter trade
                    return 1
                elif exit == 1 and state == 1:  # Exit trade
                    return 0
                else:  # Maintain current state
                    return state

            # Apply the state transition function using `map_elements`
            df = df.with_columns(
                pl.struct([entry_col, exit_col, "_state"])
                .map_elements(
                    lambda s: state_transition(s[entry_col], s[exit_col], s["_state"]),
                    return_dtype=pl.Int8,
                )
                .alias("_state_new")
            )

            # Update the state column
            df = df.with_columns(pl.col("_state_new").alias("_state"))

            # Rename the state column to the final position column
            df = df.with_columns(pl.col("_state").alias(position_col))

            # Drop intermediate columns
            df = df.drop(["_state", "_state_new"])

        return df
    
    def calcReturns(self, df) -> pl.DataFrame:
        df = df.with_columns(((pl.col("returns") * pl.col("is_long")).alias("long_returns")),
                                ((pl.col("returns") * pl.col("is_short")).alias("short_returns")))
        df = df.with_columns(
            (pl.col("long_returns") + pl.col("short_returns")).alias("strategy_returns")
        )
        df = df.with_columns((pl.col("long_returns") * self.starting_cash).alias("cash_returns"))
        df = df.with_columns(pl.col("cash_returns").cum_sum().alias("total_cash_returns"))
        df = df.with_columns(
            pl.col("strategy_returns").cum_sum().alias("cum_returns")
        )
        df = df.with_columns(pl.col("returns").cum_sum().alias("buy_and_hold"))
        return df

    def calcTradeStats(self, df: pl.DataFrame) -> pl.DataFrame:
        for direction in ("long", "short"):
            df = df.with_columns(pl.when(pl.col(f"is_{direction}").shift() == 0 & pl.col(f"is_{direction}") == 1)
                                .then(1)
                                .otherwise(0)
                                .alias(f"{direction}_entry"))
            
            df = df.with_columns(
                pl.when(pl.col(f"is_{direction}").shift() == 1 & pl.col(f"is_{direction}") == 0)
                .then(1)
                .otherwise(0)
                .alias(f"{direction}_exit")
            )
            
    def runBacktest(self):
        for symbol, df in list(self.dfs.items()):
            df = self.trackIsInTrade(df)
            df = self.calcReturns(df)

            del self.dfs[symbol]  # Delete the old dictionary.
            self.dfs[symbol] = df  # Replace it.



class TestStrat(ResearchStrat):
    def __init__(self):
        self.files = {"QQQ": "/Users/S5249272/Desktop/Python Projects/beetleMoneyMaker/localData/QQQ.parquet"}

    def initColumns(self):
        for symbol, df in list(self.dfs.items()):
            df = df.with_columns(((pl.col("close")).rolling_mean(21)).alias("sma21"))  # SMA 21
            df = df.with_columns(((pl.col("close")).rolling_mean(7)).alias("sma7"))  # SMA 7
            df = df.drop_nulls()
            del self.dfs[symbol]  # Delete the old dictionary.
            self.dfs[symbol] = df  # Replace it.

    def setSignals(self):
        for symbol, df in list(self.dfs.items()):
            df = df.with_columns(pl.when((pl.col("sma21") < pl.col("sma7")))
                                        .then(1)
                                        .otherwise(0)
                                        .alias("long_entry"))

            df = df.with_columns(pl.when(pl.col("sma21") > pl.col("sma7")).then(1).otherwise(0).alias("long_exit"))
            df = df.with_columns((pl.lit(0)).alias("short_entry"))
            df = df.with_columns((pl.lit(0)).alias("short_exit"))
            del self.dfs[symbol]
            self.dfs[symbol] = df
    



test = TestStrat()
test.starting_cash = 100_000
test.initColumns()
test.setSignals()
test.runBacktest()
pl.Config.set_tbl_cols(15)
pl.Config.set_tbl_rows(1000)
df = test.dfs["QQQ"]
total_returns = sum(df["strategy_returns"].to_numpy())
print(total_returns)

plt.figure(figsize=(10, 6))
plt.plot(df["total_cash_returns"])
#plt.plot(df["cum_returns"])
#plt.plot(df["buy_and_hold"]
plt.title("returns over time")
plt.xlabel("time")
plt.ylabel("returns")
plt.grid(True)
plt.show()


