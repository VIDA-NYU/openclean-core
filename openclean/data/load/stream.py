# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper classes and methods to stream the data from a given CSV
file in a way that is similar to applying certain openclean operators on a
given pandas DataFrame. These classes are mainly intended for simple select
and filter operations on large files that may not fit into main memory as a
data frame.
"""

import pandas as pd

from collections import Counter
from typing import List, Optional

from openclean.data.column import Column
from openclean.data.load.consumer import (
    DataFrame, Distinct, Filter, Limit, StreamConsumer, Select, Write
)
from openclean.data.load.csv import CSVFile
from openclean.function.eval.base import EvalFunction


class DataStream(object):
    """The data stream class is intended for reading and filtering large CSV
    files as data frames. The class iterates over the rows of a given CSV file.
    It allows to filer columns (select) and rows (where) using an openclean
    predicate.

    The class implements the iterrows() function and columns property of the
    pandas DataFrame. Instances of this class can be used as substitues for
    pandas DataFrames for all openclean functions that only make use of the
    iterrows() functions and the columns property for data frame arguments.
    """
    def __init__(
        self, file: CSVFile, columns: List[Column],
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the data frmae columns, the CSV reader, and the optional
        column filter.

        Parameters
        ----------
        file: CSVFile
            Handle for a CSV File.
        columns: list of Column
            List of data frame column objects
        consumer: openclean.data.load.consumer.StreamConsumer, default=None
            List of column indices in the read rows from the CSV reader that
            are included in the output (set by the select() method).
        """
        self.columns = columns
        self.file = file
        self.consumer = consumer

    def consume(self, consumer: StreamConsumer) -> pd.DataFrame:
        """Stream all rows to a consumer that has the given consumer as a sink.
        Attaches the given consumer to a (potentially) already defined consumer
        for the data stream before streaming the data.

        The returned value is the result that is returned when the given
        consumer is closed.

        Parameters
        -----------
        consumer: openclean.data.load.consumer.StreamConsumer
            Consumer for rows in the associated data stream.

        Returns
        -------
        any
        """
        return self.stream(consumer)

    def distinct(self, *args) -> Counter:
        """Get counts for all distinct values over all columns in the
        associated data stream.

        Parameters
        ----------
        args: list of int or str
            References to the column(s) for which unique values are counted.

        Returns
        -------
        collections.Counter
        """
        # Add a select clause to filter for the requested columns if the list
        # of columns is not empty.
        if len(args) > 0:
            return self.select(*args).distinct()
        # Stream all rows for a consumer that has a distinct value counter as
        # its sink.
        return self.stream(Distinct())

    def filter(self, predicate: EvalFunction, limit: Optional[int] = None):
        """Filter rows in the data stream that match a given condition. Returns
        a new data stream with a consumer that filters the rows. Currently
        expects an evaluation function as the row predicate.

        Allows to limit the number of rows in the returned data stream.

        Parameters
        ----------
        predicate: opencelan.function.eval.base.EvalFunction
            Evaluation function used to filter rows.
        limit: int, default=None
            Limit the number of rows in the filtered data stream.

        Returns
        -------
        openclean.data.load.stream.DataStream
        """
        consumer = Filter(predicate=predicate)
        if self.consumer is not None:
            consumer = self.consumer.set_consumer(consumer)
        ds = DataStream(
            file=self.file,
            columns=self.columns,
            consumer=consumer
        )
        return ds if limit is None else ds.limit(count=limit)

    def head(self, count: Optional[int] = 10) -> pd.DataFrame:
        """Return the first n rows in the data stream as a pandas data frame.
        This is a short-cut for using a pipeline of .limit() and .to_df().

        Parameters
        ----------
        count: int, default=10
            Defines the maximum number of rows in the returned data frame.

        Returns
        -------
        pd.DataFrame
        """
        return self.limit(count=count).to_df()

    def iterrows(self):
        """Simulate the iterrows() function of a pandas DataFrame as it is used
        in openclean. Returns an iterator that yields pairs of row identifier
        and value list for each rw in the streamed data frame.
        """
        # Attach the given consumer to a (potentially) existing consumer before
        # streaming all rows the the resulting consumer.
        with self.file.open(skip_header=True) as f:
            if self.consumer is not None:
                self.consumer.open(self.columns)
                for rowid, row in f:
                    try:
                        row = self.consumer.consume(rowid, row)
                        if row is not None:
                            yield rowid, row
                    except StopIteration:
                        break
                self.consumer.close()
            else:
                for rowid, row in f:
                    yield rowid, row

    def limit(self, count: int):
        """Return a data stream for the data frame that will yield at most
        the first n rows passed to it from an associated producer.

        Parameters
        ----------
        count: int
            Maximum number of rows in the returned data frame.

        Returns
        -------
        openclean.data.load.stream.DataStream
        """
        consumer = Limit(limit=count)
        if self.consumer is not None:
            consumer = self.consumer.set_consumer(consumer)
        return DataStream(
            file=self.file,
            columns=self.columns,
            consumer=consumer
        )

    def select(self, *args):
        """Select a given list of columns from the streamed data frame. Columns
        may either be referenced by their index position or their name.

        Returns a new data stream with the column filter set to the columns
        that were in the argument list.

        Parameters
        ----------
        args: list of int or string
            List of column names or index positions.

        Returns
        -------
        openclean.data.load.stream.DataStream
        """
        consumer = Select(columns=list(args))
        if self.consumer is not None:
            consumer = self.consumer.set_consumer(consumer)
        return DataStream(
            file=self.file,
            columns=self.columns,
            consumer=consumer
        )

    def stream(self, consumer):
        """Stream all rows from the associated data file to the given stream
        consumer. The returned value is the result that is returned when the
        consumer is closed.

        Parameters
        -----------
        consumer: openclean.data.load.consumer.StreamConsumer
            Consumer for rows in the associated data stream.

        Returns
        -------
        any
        """
        # Attach the given consumer to a (potentially) existing consumer before
        # streaming all rows the the resulting consumer.
        if self.consumer is not None:
            consumer = self.consumer.set_consumer(consumer)
        consumer.open(self.columns)
        with self.file.open(skip_header=True) as f:
            for rowid, row in f:
                try:
                    consumer.consume(rowid, row)
                except StopIteration:
                    break
        return consumer.close()

    def to_df(self) -> pd.DataFrame:
        """Collect all rows in the stream that are yielded by the associated
        consumer into a pandas data frame.

        Returns
        -------
        pd.DataFrame
        """
        return self.stream(DataFrame())

    def where(self, predicate: EvalFunction, limit: Optional[int] = None):
        """Filter rows in the data stream that match a given condition. Returns
        a new data stream with a consumer that filters the rows. Currently
        expects an evaluation function as the row predicate.

        Allows to limit the number of rows in the returned data stream.

        This is a synonym for the filter() method.

        Parameters
        ----------
        predicate: opencelan.function.eval.base.EvalFunction
            Evaluation function used to filter rows.
        limit: int, default=None
            Limit the number of rows in the filtered data stream.

        Returns
        -------
        openclean.data.load.stream.DataStream
        """
        return self.filter(predicate=predicate, limit=limit)

    def write(
        self, filename: str, delim: Optional[str] = None,
        compressed: Optional[bool] = None
    ):
        """Write the rows in the data stream to a given CSV file.

        Parameters
        ----------
        filename: string
            Path to a CSV file output file on the local file system.
        delim: string, default=None
            The column delimiter used for the written CSV file.
        compressed: bool, default=None
            Flag indicating if the file contents of the created file are to be
            compressed using gzip.
        """
        file = CSVFile(filename=filename, delim=delim, compressed=compressed)
        with file.write() as writer:
            self.consume(Write(writer))


# -- Factory for data streams -------------------------------------------------

def stream(
    filename: str, delim: Optional[str] = None,
    compressed: Optional[bool] = None
) -> DataStream:
    """Read a CSV file as a data stream. This is a helper method that is
    intended to read and filter large CSV files.

    Parameters
    ----------
    filename: string
        Path to CSV file on the local file system.
    delim: string, default=None
        The column delimiter used in the CSV file.
    compressed: bool, default=None
        Flag indicating if the file contents have been compressed using
        gzip.

    Returns
    -------
    openclean.data.load.DataStream
    """
    file = CSVFile(filename=filename, delim=delim, compressed=compressed)
    with file.open(skip_header=False) as reader:
        columns = list()
        for col_name in reader.header():
            cid = len(columns)
            col = Column(colid=cid, name=col_name.strip(), colidx=cid)
            columns.append(col)
    return DataStream(file=file, columns=columns)
