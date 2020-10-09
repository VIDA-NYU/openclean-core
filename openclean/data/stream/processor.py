# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operators in a data stream pipeline represent the actors in the definition
of the pipeline. Each operator provides the functionality (factory pattern) to
instantiate the operator for a given data stream before a data streaming
pipeline is executed.

The stream processor represents a data stream that is associated with a stream
processing pipeline to filer, manipulate, or profile rows in teh data stream.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import List, Optional

import pandas as pd

from openclean.data.column import ColumnName, ColumnRef
from openclean.data.select import select_clause
from openclean.data.stream.base import DatasetStream
from openclean.data.stream.consumer import DataFrame, StreamConsumer, Select


# -- Stream operators ---------------------------------------------------------

class StreamOperator(metaclass=ABCMeta):
    """Stream operators represent definitions of actors in a data stream
    processing pipeline. Each operator implements the open method to create
    a prepared instance (consumer) for the operator that is used in a stream
    processing pipeline to filter, manipulate or profile data stream rows.
    """
    @abstractmethod
    def open(
        self, ds: StreamProcessor, pipeline: List[StreamOperator]
    ) -> StreamConsumer:
        """Signal that the consumer is about to receive the first row in the
        data stream. Contains the list of column names that define the schema
        of the rows in the stream.

        Parameters
        ----------
        schema: list of column names
            Schema for rows in the data stream.
        """
        raise NotImplementedError()  # pragma: no cover


class DataFrameOperator(StreamOperator):
    """
    """
    def open(
        self, ds: StreamProcessor, pipeline: List[StreamOperator]
    ) -> DataFrameOperator:
        """

        Parameters
        ----------

        Returns
        -------

        """
        return DataFrame(columns=ds.columns)


class SelectOperator(StreamOperator):
    """Definition of a column select operator for a data stream processing
    pipeline.
    """
    def __init__(self, columns=List[ColumnRef]):
        """Initialize the list of column references for those columns from the
        input schema that will be part of the output schema.

        Parameters
        ----------
        columns: list int or string
            References to columns in the input schema for this operatir.
        """
        self.columns = columns

    def open(
        self, ds: StreamProcessor, pipeline: List[StreamOperator]
    ) -> Select:
        """

        Parameters
        ----------

        Returns
        -------

        """
        colnames, colidxs = select_clause(ds, self.columns)
        if pipeline:
            op = pipeline[0]
            ds = StreamProcessor(
                reader=ds.reader,
                columns=colnames,
                pipeline=ds.pipeline + [op]
            )
            return Select(
                columns=colidxs,
                consumer=op.open(ds=ds, pipeline=pipeline[1:])
            )
        else:
            return Select(columns=colidxs)


# -- Stream processor ---------------------------------------------------------

class StreamProcessor(object):
    """The data stream class is intended for reading and filtering large CSV
    files as data frames. The class iterates over the rows of a given CSV file.
    It allows to define a processing pipeline of stream operators to fitler,
    manipulate, and profile the rows in the data stream .

    The class implements the iterrows() function and columns property of the
    pandas DataFrame. Instances of this class can be used as substitues for
    pandas DataFrames for all openclean functions that only make use of the
    iterrows() functions and the columns property for data frame arguments.
    """
    def __init__(
        self,
        reader: DatasetStream,
        columns: Optional[List[ColumnName]] = None,
        pipeline: Optional[StreamOperator] = None
    ):
        """Initialize the data stream reader, schema information for the
        streamed rows, and the optional
        column filter.

        Parameters
        ----------
        reader: openclean.data.stream.base.DatasetReader
            Reader for the data stream.
        pipeline: ???
        """
        self.reader = reader
        self.columns = columns if columns is not None else reader.columns
        self.pipeline = pipeline if pipeline is not None else list()

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
        and value list for each row in the streamed data frame.
        """
        if self.pipeline:
            ds = StreamProcessor(reader=self.reader)
            consumer = self.pipeline[0].open(ds, self.pipeline[1:])
            for rowid, row in self.reader.iterrows():
                try:
                    row = consumer.consume(rowid, row)
                    if row is not None:
                        yield rowid, row
                except StopIteration:
                    break
            consumer.close()
        else:
            return self.reader.iterrows()

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
        openclean.data.stream.processor.StreamProcessor
        """
        op = SelectOperator(columns=list(args))
        return StreamProcessor(
            reader=self.reader,
            columns=self.columns,
            pipeline=self.pipeline + [op]
        )

    def run(self):
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
        if self.pipeline:
            ds = StreamProcessor(reader=self.reader)
            consumer = self.pipeline[0].open(ds, self.pipeline[1:])
            for rowid, row in self.reader.iterrows():
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
        op = DataFrameOperator()
        return StreamProcessor(
            reader=self.reader,
            columns=self.columns,
            pipeline=self.pipeline + [op]
        ).run()

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
