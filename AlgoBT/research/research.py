import numpy as np
import scipy as sp
import polars as pl
import hyppo as hp
import future
import matplotlib.pyplot as plt
from typing import Literal
from plotter import Plotter

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
    
    def trackIsInTrade(self, df, col_names: list) -> pl.DataFrame:
        for colm in col_names:
            df = df.with_columns(pl.when(pl.col(colm) == 1) & (colm == "long_entry"))
    
    def runBacktest(self):
        for symbol, df in list(self.dfs.items()):
            df = df.with_columns(pl.col("returns") * pl.col("long_signal"))
            


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
            df = df.with_columns(pl.when((pl.col("sma21") < pl.col("sma7")) &
                                        (pl.col("sma21").shift(-1) > pl.col("sma7").shift(-1)))
                                        .then(1)
                                        .otherwise(0)
                                        .alias("long_signal"))

            df = df.with_columns(pl.when(pl.col("sma21") > pl.col("sma7")).alias("exit_signal"))
            self.trackIsInTrade(self, ["long_signal"])
            del self.dfs[symbol]
            self.dfs[symbol] = df
    
    

test = TestStrat()
test.initColumns()
test.setSignals()
test.runBacktest


