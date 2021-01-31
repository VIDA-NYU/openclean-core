# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper classes to read data frames from CSV files."""

import pandas as pd

from openclean.data.stream.base import DataIterator, DataReader


class DataFrameReader(DataIterator):
    """Iterable reader for rows in a pandas data frame. Instances of this class
    act as a context manager.
    """
    def __init__(self, df: pd.DataFrame):
        """Initialize the data frame.

        Parameters
        ----------
        df: pd.DataFramer
            Data frame that is streamed.
        """
        self.df = df
        # Read index for data frame rows.
        self.rowidx = 0

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated file handle when the context manager exits."""
        return False

    def __iter__(self):
        """Return object for row iteration."""
        return self

    def __next__(self):
        """Return next row from CSV reader. The CSV reader will raise a
        StopIteration error when the end of the file is reached.
        """
        if self.rowidx < self.df.shape[0]:
            rowid = self.df.index[self.rowidx]
            row = list(self.df.iloc[self.rowidx])
            self.rowidx += 1
            return rowid, row
        raise StopIteration()


class DataFrameStream(DataReader):
    """Wrapper for pandas data frames that act as a data stream reader."""
    def __init__(self, df: pd.DataFrame):
        """Set the data frame that is returned as the data stream.

        Parameters
        ----------
        df: pd.DataFramer
            Data frame that is streamed.
        """
        super(DataFrameStream, self).__init__(columns=list(df.columns))
        self.df = df

    def open(self) -> DataFrameReader:
        """Get an iterator for the associated data frame.

        Returns
        -------
        openclean.data.stream.csv.CSVReader
        """
        return DataFrameReader(df=self.df)
