from typing import Optional
import polars as pl
from research import Research
import numpy as np


class DataCleaner():
    def __init__(self, timeframe_to_agg, lowest_timeframe,
                polars_df: Optional[pl.DataFrame] = None, file_path: Optional["str"] = None):
        """initiatlize DataCleaner Class
        at least one polars_df or file_path must be provided.

        :param timeframe_to_agg: timeframe to aggregate polars df to
        :param lowest_timeframe: _description_
        :param polars_df: pass a polars df to clean and aggregate. Do not pass a polars df and filepath
        :param file_path: the file path to load the data, if polars_df is not passed
        """
        self.target_timeframe = timeframe_to_agg
        if polars_df is None and file_path is None:
            raise ValueError("At least one polars_df or file_path must be provided.")
        if polars_df is None:
            self.df = self._loadData(file_path)
        else:
            self.df = polars_df

    def _loadData(self, path):
        """loads data from a stored file based on file path.
           Valid file types are csv, parquet, and json

        :param path: Path to file storage. Must be suffixed with apropriate suffix of file type.
        :return: Returns loaded df
        """
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
        except Exception as e:
            raise ValueError(f"Error loading {file_type} file at path '{path}': {e}")
        return df

    def cleanHighLows(self, stdvs: Optional[int] = 3) -> None:
        """cleans dataframe highs and lows using standard deviation

        :param stdvs: _description_, defaults to 3
        :raises ValueError: _description_
        """

        try:
            df = self.df.with_columns(abs((pl.col("high") - (pl.col("close") + pl.col("open")) / 2)
                                    / pl.col("high")).alias("high_dif")
            )
            df = self.df.with_columns(
                abs((pl.col("low") - ((pl.col("close") + pl.col("open")) / 2)) / pl.col("high")).alias("low_dif")
            )

        except Exception as e:
            raise ValueError(
                "dataframe is invalid. check that provided df has an open, high, low, and close columns,"
                "and that they are lower-case and coantain numerical values only."
                f"Exception: {e}"
            )

        low_std = df["low_dif"].std()
        high_std = df["high_dif"].std()
        high_outlier = df["high_dif"].mean() + high_std * stdvs
        low_outlier = df["low_dif"].mean() + low_std * stdvs

        df = df.with_columns(pl.when(pl.col("high_dif") < high_outlier)
                            .then(pl.col("high")
                            .otherwise(max(pl.col("close"), pl.col("open")))
                            .alias("high")))
        df = df.with_columns(pl.when(pl.col("low_dif") < low_outlier)
                            .then(pl.col("low")
                            .otherwise(min(pl.col("close"), pl.col("open")))
                            .alias("low")))
        self.df = df

