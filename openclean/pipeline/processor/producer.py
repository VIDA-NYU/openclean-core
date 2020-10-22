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
from abc import abstractmethod
from collections import Counter
from typing import Dict, List, Optional, Tuple, Type, Union

import pandas as pd

from openclean.data.types import ColumnName, ColumnRef
from openclean.data.select import select_clause
from openclean.data.stream.base import DatasetStream
from openclean.data.stream.csv import CSVFile
from openclean.data.types import Scalar
from openclean.function.eval.base import EvalFunction
from openclean.operator.transform.update import get_update_function
from openclean.data.stream.base import StreamConsumer
from openclean.pipeline.consumer.collector import Distinct, RowCount
from openclean.pipeline.consumer.producer import Filter, Limit, Select, Update
from openclean.pipeline.processor.base import StreamProcessor
from openclean.pipeline.processor.collector import (
    CollectOperator, DataFrameOperator, WriteOperator
)
from openclean.profiling.base import ProfilingFunction
from openclean.profiling.datatype.convert import DatatypeConverter


# -- Pipeline operators -------------------------------------------------------

class ProducingOperator(StreamProcessor):
    """Stream operator that yields a consumer that passes processed rows on to
    a downstream consumer. This is a abstract class that requires the
    create_consumer method to be implemented that returns an instance of the
    stream consumer.
    """
    @abstractmethod
    def create_consumer(
        self, ds: DatasetStream
    ) -> Tuple[StreamConsumer, List[ColumnName]]:
        """Create an instance of the stream consumer. Also returns the columns
        in the schema of the rows that are yielded by the consumer.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input. The data
            stream can be used to prepare any associated evaluation functions
            that are used by the returned consumer.

        Returns
        -------
        openclean.data.stream.base.StreamConsumer, list of string
        """
        raise NotImplementedError()  # pragma: no cover

    def open(
        self, ds: DatasetStream,
        schema: List[str],
        upstream: Optional[List[StreamProcessor]] = None,
        downstream: Optional[List[StreamProcessor]] = None
    ) -> StreamConsumer:
        """Return a stream consumer that processed rows and passes the results
        on to an optional downstream consumer.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input. Use this
            stream in combination with any upstream operators to prepare any
            associated evaluation functions that are used by the returned
            consumer.
        schema: list of string
            List of column names in the data stream schema.
        upstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of upstream operators for the received data stream.
        downstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of downstream operators for the generated consumer.

        Returns
        -------
        openclean.data.stream.base.StreamConsumer
        """
        # Get an instance of the consumer. Defer setting the downstream
        # consumer.
        upstream_ds = DataPipeline(ds, columns=schema, pipeline=upstream)
        consumer, columns = self.create_consumer(ds=upstream_ds)
        if downstream:
            # Ensure that the list of upstream operators is not None.
            ustream = upstream if upstream is not None else list()
            # Create instance of the downstream consumer if the pipeline is
            # not empty.
            op = downstream[0]
            consumer.consumer = op.open(
                ds=ds,
                schema=columns,
                upstream=ustream + [self],
                downstream=downstream[1:]
            )
        return consumer


class FilterOperator(ProducingOperator):
    """Definition of a row filter operator for a data stream processing
    pipeline.
    """
    def __init__(
        self, predicate=EvalFunction, truth_value: Optional[Scalar] = True
    ):
        """Initialize the evaluation function that is used a predicate to
        filter rows in the data stream.

        Parameters
        ----------
        predicate: openclean.function.eval.base.EvalFunction
            Evaluation function that is used as the predicate for filtering
            data stream rows. The function will be prepared in the
            create_consumer method.
        truth_value: scalar, defaut=True
            Return value of the predicate that signals that the predicate is
            satisfied by an input value.
        """
        self.predicate = predicate
        self.truth_value = truth_value

    def create_consumer(
        self, ds: DatasetStream
    ) -> Tuple[Filter, List[ColumnName]]:
        """Create a filter consumer that will only yield row that satisfy the
        associated predicate.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input. The data
            stream can be used to prepare any associated evaluation functions
            that are used by the returned consumer.

        Returns
        -------
        openclean.pipeline.consumer.producer.Filter
        """
        # Prepare the predicate.
        prep_pred = self.predicate.prepare(ds)
        consumer = Filter(predicate=prep_pred, truth_value=self.truth_value)
        return consumer, ds.columns


class LimitOperator(ProducingOperator):
    """Definition of a row limit operator for a data stream processing
    pipeline.
    """
    def __init__(self, limit: int):
        """Initialize the maximum number of rows that the created cunsumer will
        yield.

        Parameters
        ----------
        limit: int
            Maximum number of rows that consumers for this operator will pass
            on to the downstream consumer.
        """
        self.limit = limit

    def create_consumer(
        self, ds: DatasetStream
    ) -> Tuple[Limit, List[ColumnName]]:
        """Return a consumer that limits the number of rows that are yoelded
        or passed on to the downstream consumer.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input. The data
            stream can be used to prepare any associated evaluation functions
            that are used by the returned consumer.

        Returns
        -------
        openclean.pipeline.consumer.producer.Limit
        """
        return Limit(limit=self.limit), ds.columns


class SelectOperator(ProducingOperator):
    """Definition of a column select operator for a data stream processing
    pipeline.
    """
    def __init__(self, columns: List[ColumnRef]):
        """Initialize the list of column references for those columns from the
        input schema that will be part of the output schema.

        Parameters
        ----------
        columns: list int or string
            References to columns in the input schema for this operator.
        """
        self.columns = columns

    def create_consumer(
        self, ds: DatasetStream
    ) -> Tuple[Select, List[ColumnName]]:
        """Return a stream consumer that filters columns from the rows in a
        data stream.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input. The data
            stream can be used to prepare any associated evaluation functions
            that are used by the returned consumer.

        Returns
        -------
        openclean.pipeline.consumer.producer.Select
        """
        # Get names and index positions for the filtered columns.
        colnames, colidxs = select_clause(ds.columns, self.columns)
        return Select(columns=colidxs), colnames


class UpdateOperator(ProducingOperator):
    """Definition of a update select operator for a data stream processing
    pipeline.
    """
    def __init__(self, columns: List[ColumnRef], func: EvalFunction):
        """Initialize the list of column references for those columns from the
        input schema that will be updated and the update function.

        Parameters
        ----------
        columns: list int or string
            References to columns in the input schema for this operator.
        func: openclean.function.eval.base.EvalFunction
            Evaluation function that is used to update rows in the data stream.
            The function will be prepared in the create_consumer method.
        """
        self.columns = columns
        self.func = func

    def create_consumer(
        self, ds: DatasetStream
    ) -> Tuple[Update, List[ColumnName]]:
        """Return a stream consumer that updates rows in a data stream using
        the associated update function.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input. The data
            stream can be used to prepare any associated evaluation functions
            that are used by the returned consumer.

        Returns
        -------
        openclean.pipeline.consumer.producer.Update
        """
        # Get index positions for the updated columns.
        _, colidxs = select_clause(ds.columns, self.columns)
        # Prepate the update funnction
        prep_func = self.func.prepare(ds)
        # Return instance of the update consumer.
        return Update(columns=colidxs, func=prep_func), ds.columns


# -- Data pipeline ------------------------------------------------------------


"""Type alias for column profiler specifications."""
ColumnProfiler = Union[ColumnRef, Tuple[ColumnRef, ProfilingFunction]]
ProfilerSpecs = Union[ColumnProfiler, List[ColumnProfiler]]


class DataPipeline(DatasetStream):
    """The data pipeline allows to iterate over the rows that are the result of
    streaming an input data set through a pipeline of stream operators.

    The class implements the iterrows() function and columns property of the
    pandas DataFrame. Instances of this class can be used as substitues for
    pandas DataFrames for all openclean functions that only make use of the
    iterrows() functions and the columns property for data frame arguments.
    """
    def __init__(
        self, reader: DatasetStream,
        columns: Optional[List[ColumnName]] = None,
        pipeline: Optional[StreamProcessor] = None
    ):
        """Initialize the data stream reader, schema information for the
        streamed rows, and the optional pipeline operators.

        Parameters
        ----------
        reader: openclean.data.stream.base.DatasetReader
            Reader for the data stream.
        columns: list of string
            List of column names for the schema of the data stream rows.
        pipeline: list of openclean.data.stream.processor.StreamProcessor,
                default=None
            List of operators in the pipeline fpr this stream processor.

        """
        super(DataPipeline, self).__init__(
            columns=columns if columns is not None else reader.columns
        )
        self.reader = reader
        self.pipeline = pipeline if pipeline is not None else list()

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated file handle when the context manager exits."""
        return False

    def append(
        self, op: StreamProcessor, columns: Optional[List[ColumnName]] = None
    ) -> DataPipeline:
        """Return a modified stream processer with the given operator appended
        to the stream pipeline.

        Parameters
        ----------
        op: openclean.pipeline.processor.base.StreamProcessor
            Stream operator that is appended to the pipeline of the returned
            stream processor.
        columns: list of string, default=None
            Optional (modified) list of column names for the schema of the data
            stream rows.

        Returns
        -------
        openclean.pipeline.processor.DataPipeline
        """
        return DataPipeline(
            reader=self.reader,
            columns=columns if columns is not None else self.columns,
            pipeline=self.pipeline + [op]
        )

    def count(self) -> int:
        """Count the number of rows in a data stream.

        Returns
        -------
        int
        """
        return self.stream(CollectOperator(RowCount))

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
        op = CollectOperator(Distinct)
        # If optional list of columns is given append a select operation first
        # to filter on those columns before running the data stream.
        columns = list(args)
        if len(columns) > 0:
            return self.select(*args).stream(op)
        return self.stream(op)

    def filter(
        self, predicate: EvalFunction, truth_value: Optional[Scalar] = True,
        limit: Optional[int] = None
    ) -> DataPipeline:
        """Filter rows in the data stream that match a given condition. Returns
        a new data stream with a consumer that filters the rows. Currently
        expects an evaluation function as the row predicate.

        Allows to limit the number of rows in the returned data stream.

        Parameters
        ----------
        predicate: opencelan.function.eval.base.EvalFunction
            Evaluation function used to filter rows.
        truth_value: scalar, defaut=True
            Return value of the predicate that signals that the predicate is
            satisfied by an input value.
        limit: int, default=None
            Limit the number of rows in the filtered data stream.

        Returns
        -------
        openclean.pipeline.processor.DataPipeline
        """
        # Create a new stream processor with a filter operator appended to the
        # pipeline.
        op = FilterOperator(predicate=predicate, truth_value=truth_value)
        ds = self.append(op)
        # Append a limit operator to the returned dataset if a limit is given.
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
            consumer = self.pipeline[0].open(
                ds=self.reader,
                schema=self.reader.columns,
                upstream=[],
                downstream=self.pipeline[1:]
            )
            for rowid, row in self.reader.iterrows():
                try:
                    row = consumer.consume(rowid, row)
                    if row is not None:
                        yield rowid, row
                except StopIteration:
                    break
            consumer.close()
        else:
            for rowid, row in self.reader.iterrows():
                yield rowid, row

    def limit(self, count: int) -> DataPipeline:
        """Return a data stream for the data frame that will yield at most
        the first n rows passed to it from an associated producer.

        Parameters
        ----------
        count: int
            Maximum number of rows in the returned data frame.

        Returns
        -------
        openclean.pipeline.processor.DataPipeline
        """
        return self.append(LimitOperator(limit=count))

    def open(self) -> DataPipeline:
        """Return reference to self when the pipeline is opened.

        Returns
        -------
        openclean.pipeline.processor.producer.DataPipeline
        """
        return self

    def profile(
        self, profilers: Optional[ProfilerSpecs] = None,
        default_profiler: Optional[Type] = None
    ) -> List[Dict]:
        """Profile one or more columns in the data stream. Returns a list of
        profiler results for each profiled column.

        By default each column in the data stream is profiled independently
        using the default stream profiler. The optional list of profilers
        allows to override the default behavior by providing a list of column
        references (with optional profiler function). If only a column
        reference is given the default stream profiler is used for the
        referenced column.

        Parameters
        ----------
        profilers: int, string, tuple, or list of tuples of column reference
                and openclean.profiling.base.ProfilingFunction, default=None
            Specify he list of columns that are profiled and the profiling
            function. If only a column reference is given (not a tuple) the
            default stream profiler is used for profiling the column.
        default_profiler: class, default=None
            Class object that is instanciated as the profiler for columns
            that do not have a profiler instance speicified for them.

        Returns
        -------
        list
        """
        # Ensure that profilers is a list.
        if profilers is not None and not isinstance(profilers, list):
            profilers = [profilers]
        from openclean.profiling.dataset import ProfilingOperator
        return self.stream(
            ProfilingOperator(
                profilers=profilers,
                default_profiler=default_profiler
            )
        )

    def run(self):
        """Stream all rows from the associated data file to the data pipeline
        that is associated with this processor. If an optional operator is
        given, that operator will be appended to the current pipeline before
        execution.

        The returned value is the result that is returned when the consumer is
        generated for the pipeline is closed after processing the data stream.

        Returns
        -------
        any
        """
        # We only need to iterate over the data stream if the pipeline has at
        # least one operator. Otherwise the instantiated pipeline does not have
        # any consumer that coule generate a result.
        if not self.pipeline:
            return None
        # Instantiate the consumer for the defined pipeline.
        consumer = self.pipeline[0].open(
            ds=self.reader,
            schema=self.reader.columns,
            upstream=[],
            downstream=self.pipeline[1:]
        )
        # Stream all rows to the consumer. THe returned result is the result
        # returned when the consumer is closed by the reader.
        return self.reader.stream(consumer)

    def select(self, *args) -> DataPipeline:
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
        openclean.pipeline.processor.DataPipeline
        """
        return self.append(SelectOperator(columns=list(args)))

    def stream(self, op: StreamProcessor):
        """Stream all rows from the associated data file to the data pipeline
        that is associated with this processor. The given operator is appended
        to the current pipeline before execution.

        The returned value is the result that is returned when the consumer is
        generated for the pipeline is closed after processing the data stream.

        Parameters
        -----------
        op: openclean.pipeline.processor.base.StreamProcessor
            Stream operator that is appended to the current pipeline
            for execution.

        Returns
        -------
        any
        """
        return self.append(op).run()

    def to_df(self) -> pd.DataFrame:
        """Collect all rows in the stream that are yielded by the associated
        consumer into a pandas data frame.

        Returns
        -------
        pd.DataFrame
        """
        return self.stream(DataFrameOperator())

    def typecast(
        self, converter: Optional[DatatypeConverter] = None
    ) -> DataPipeline:
        """Typecast operator that converts cell values in data stream rows to
        different raw types that are represented by the given type converter.

        Parameters
        ----------
        converter: openclean.profiling.datatype.convert.DatatypeConverter,
                default=None
            Datatype converter for values data stream. Uses the default
            converter if no converter is given.

        Returns
        -------
        openclean.pipeline.processor.DataPipeline
        """
        from openclean.profiling.datatype.operator import TypecastOperator
        return self.append(TypecastOperator(converter=converter))

    def update(self, *args) -> DataPipeline:
        """Update rows in a data frame. Expects a list of columns that are
        updated. The last argument is expected to be an update function that
        accepts as many arguments as there are columns in the argument list.

        Raises a Value error if not enough arguments (at least two) are given.

        Parameters
        ----------
        args: list of int or string
            List of column names or index positions.

        Returns
        -------
        openclean.data.stream.processor.StreamProcessor
        """
        args = list(args)
        if len(args) < 1:
            raise ValueError('not enough arguments for update')
        columns = args[:-1]
        func = get_update_function(func=args[-1], columns=columns)
        return self.append(UpdateOperator(columns=columns, func=func))

    def where(
        self, predicate: EvalFunction, limit: Optional[int] = None
    ) -> DataPipeline:
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
        openclean.pipeline.processor.DataPipeline
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
        file = CSVFile(
            filename=filename,
            delim=delim,
            compressed=compressed,
            write=True
        )
        return self.stream(WriteOperator(file=file))
