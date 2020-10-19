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
from collections import Counter
from typing import Dict, List, Optional, Tuple, Type, Union

import pandas as pd

from openclean.data.types import ColumnName, ColumnRef
from openclean.data.select import select_clause
from openclean.data.stream.base import DatasetStream
from openclean.data.stream.consumer import (
    Count, DataFrame, Distinct, Filter, Limit, Profile, StreamConsumer,
    Select, Update, Write
)
from openclean.data.stream.csv import CSVFile
from openclean.data.types import Scalar
from openclean.function.eval.base import EvalFunction
from openclean.operator.transform.update import get_update_function
from openclean.profiling.base import ProfilingFunction
from openclean.profiling.column import DefaultStreamProfiler


"""Type alias for column profiler specifications."""
ColumnProfiler = Union[ColumnRef, Tuple[ColumnRef, ProfilingFunction]]
ProfilerSpecs = Union[ColumnProfiler, List[ColumnProfiler]]


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
        """Factory pattern for stream consumer. Returns an instance of the
        stream consumer that corresponds to the action that is defined by the
        stream operator.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input. Can be used to prepare evaluation funcitons that
            accept a dataset stream.
        pipeline: list of openclean.data.stream.processor.StreamOperator
            List of downstream operators for the generated consumer.

        Returns
        -------
        openclean.data.stream.consumer.StreamConsumer
        """
        raise NotImplementedError()  # pragma: no cover


class ProducingOperator(StreamOperator):
    """Stream operator that yields a consumer that passes processed rows on to
    a downstream consumer. This is a abstract class that requires the
    create_consumer method to be implemented that returns an instance of the
    stream consumer.
    """
    @abstractmethod
    def create_consumer(
        self, ds: StreamProcessor
    ) -> Tuple[StreamConsumer, List[ColumnName]]:
        """Create an instance of the stream consumer. Also returns the columns
        in the schema of the rows that are yielded by the consumer.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input. Defines the schema of the input rows for the
            returned consumer.

        Returns
        -------
        openclean.data.stream.consumer.StreamConsumer, list of string
        """
        raise NotImplementedError()  # pragma: no cover

    def open(
        self, ds: StreamProcessor, pipeline: List[StreamOperator]
    ) -> StreamConsumer:
        """Return a stream consumer that processed rows and passes the results
        on to an optional downstream consumer.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input. Defines the schema of the input rows for the
            returned consumer.
        pipeline: list of openclean.data.stream.processor.StreamOperator
            List of downstream operators for the generated consumer.

        Returns
        -------
        openclean.data.stream.consumer.StreamConsumer
        """
        # Get an instance of the consumer. Defer setting the downstream
        # consumer.
        consumer, columns = self.create_consumer(ds)
        if pipeline:
            # Create instance of the downstream consumer if the pipeline is
            # not empty.
            op = pipeline[0]
            consumer.consumer = op.open(
                ds=ds.append(op=self, columns=columns),
                pipeline=pipeline[1:]
            )
        return consumer


class CollectOperator(StreamOperator):
    """Stream operator that represents the endpoint in a data stream. This is
    a generic operator for any consumer that does not require access to the
    data stream columns when it is instantiated.
    """
    def __init__(self, cls: StreamConsumer, *args, **kwargs):
        """Initialize the class of the stream consumer that is instantiated
        when the open method is called. The class constructor will receive any
        additional arguments that are passed to this constructor.

        Parameters
        ----------
        cls: class definition of openclean.data.stream.consumer.StreamConsumer
            Class of the consumer that is instantiated by the operator.
        args: variable argument list
            Additional arguments for the consumer class constructor.
        kwrgs: variable keyword arguments
            Additional keywrod arguments for the consumer class constructor.
        """
        self.cls = cls
        self.args = args
        self.kwargs = kwargs

    def open(
        self, ds: StreamProcessor, pipeline: List[StreamOperator]
    ) -> StreamConsumer:
        """Create an instance of the associated consumer class as the sink in a
        data stream processing pipeline. Will ignore any given downstream
        operators.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input. Ignored.
        pipeline: list of openclean.data.stream.processor.StreamOperator
            List of downstream operators for the generated consumer. Ignored.

        Returns
        -------
        openclean.data.stream.consumer.StreamConsumer
        """
        return self.cls(*self.args, **self.kwargs)


class DataFrameOperator(StreamOperator):
    """Stream operator that returns a data frame collector as the generated
    consumer. Ignores any given downstram operators.
    """
    def open(
        self, ds: StreamProcessor, pipeline: List[StreamOperator]
    ) -> DataFrame:
        """Create a data frame collector as the sink in a data stream
        processing pipeline. Will ignore any given downstream operators.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input.
        pipeline: list of openclean.data.stream.processor.StreamOperator
            List of downstream operators for the generated consumer. Ignored.

        Returns
        -------
        openclean.data.stream.consumer.DataFrame
        """
        return DataFrame(columns=ds.columns)


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
        self, ds: StreamProcessor
    ) -> Tuple[Filter, List[ColumnName]]:
        """Create a filter consumer that will only yield row that satisfy the
        associated predicate.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input. Used to prepare the predicate.

        Returns
        -------
        openclean.data.stream.consumer.Filter
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
        self, ds: StreamProcessor
    ) -> Tuple[Limit, List[ColumnName]]:
        """Return a consumer that limits the number of rows that are yoelded
        or passed on to the downstream consumer.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input.

        Returns
        -------
        openclean.data.stream.consumer.Limit
        """
        return Limit(limit=self.limit), ds.columns


class ProfilingOperator(StreamOperator):
    def __init__(
        self, profilers: Optional[ColumnProfiler] = None,
        default_profiler: Optional[Type] = None
    ):
        """Profiling operator one or more columns in the data stream. By
        default all columns in the data stream are profiled independently using
        the default stream profiler. The optional list of profilers allows to
        override the default behavior by providing a list of column references
        and (optional) profiler functions.

        Parameters
        ----------
        profilers: list of tuples of column reference and
                openclean.profiling.base.ProfilingFunction, default=None
            Specify he list of columns that are profiled and the profiling
            function. If only a column reference is given (not a tuple) the
            default stream profiler is used for profiling the column.
        default_profiler: class, default=None
            Class object that is instanciated as the profiler for columns
            that do not have a profiler instance speicified for them.
        """
        self.profilers = profilers
        if default_profiler is None:
            default_profiler = DefaultStreamProfiler
        self.default_profiler = default_profiler

    def open(
        self, ds: StreamProcessor, pipeline: List[StreamOperator]
    ) -> StreamConsumer:
        """Factory pattern for stream profiling consumers. Creates an instance
        of a stream profiler for each column that was selected for profiling.
        If no profilers were specified at object instantiation all columns will
        be profiled.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input. Can be used to prepare evaluation funcitons that
            accept a dataset stream.
        pipeline: list of openclean.data.stream.processor.StreamOperator
            List of downstream operators for the generated consumer.

        Returns
        -------
        openclean.data.stream.consumer.Profile
        """
        # Create a list of (column index, profiling function)-pairs that is
        # passed to the profiling cnsumer. That consumer will (i) open the
        # profilers, (ii) dispatch extracted row values to the consumers, and
        # (iii) generate the result from the profiling functions.
        consumers = list()
        if self.profilers is None:
            # If no profilers were specified at object creation all columns are
            # profiled.
            for i, name in enumerate(ds.columns):
                consumers.append((i, name, self.default_profiler()))
        else:
            # Create profilers only for columns included in the profilers list
            # that was initialized at object creation.
            for p in self.profilers:
                # There are two types of values allowed in the profiler list.
                # Tuples that are (column reference, profiler function)-pairs
                # or column references only.
                if isinstance(p, tuple):
                    col, profiler = p
                else:
                    col = p
                    profiler = self.default_profiler()
                name, colidx = select_clause(ds.columns, col)
                consumers.append((colidx[0], name[0], profiler))
        return Profile(profilers=consumers)


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
        self, ds: StreamProcessor
    ) -> Tuple[Select, List[ColumnName]]:
        """Return a stream consumer that filters columns from the rows in a
        data stream.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input. Defines the schema of the input rows for the
            returned consumer.

        Returns
        -------
        openclean.data.stream.consumer.Select
        """
        # Get names and index positions for the filtered columns.
        colnames, colidxs = select_clause(ds, self.columns)
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
        self, ds: StreamProcessor
    ) -> Tuple[Update, List[ColumnName]]:
        """Return a stream consumer that updates rows in a data stream using
        the associated update function.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input.

        Returns
        -------
        openclean.data.stream.consumer.Update
        """
        # Get index positions for the updated columns.
        _, colidxs = select_clause(ds, self.columns)
        # Prepate the update funnction
        prep_func = self.func.prepare(ds)
        # Return instance of the update consumer.
        return Update(columns=colidxs, func=prep_func), ds.columns


class WriteOperator(StreamOperator):
    """Operator that writes the rows in a data stream to a file on the local
    file system.
    """
    def __init__(self, file):
        """Initialize the reference to the output file.

        Parameters
        ----------
        file: openclean.data.stream.csv.CSVFile
            Output CSV file for all rows in the data stream that are passed to
            a consumer for this operator.
        """
        self.file = file

    def open(
        self, ds: StreamProcessor, pipeline: List[StreamOperator]
    ) -> Write:
        """Create a consumer that writes all rows that it receives to the
        associated CSV file.

        Parameters
        ----------
        ds: openclean.data.stream.StreamProcessor
            Processor for the data stream that the created consumer will
            receive as input. Defines the header of the output file.
        pipeline: list of openclean.data.stream.processor.StreamOperator
            List of downstream operators for the generated consumer. Ignored.

        Returns
        -------
        openclean.data.stream.consumer.Write
        """
        return Write(self.file.write(header=ds.columns))


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
        columns: list of string
            List of column names for the schema of the data stream rows.
        pipeline: list of openclean.data.stream.processor.StreamOperator,
                default=None
            List of operators in the pipeline fpr this stream processor.

        """
        self.reader = reader
        self.columns = columns if columns is not None else reader.columns
        self.pipeline = pipeline if pipeline is not None else list()

    def append(
        self, op: StreamOperator, columns: Optional[List[ColumnName]] = None
    ) -> StreamProcessor:
        """Return a modified stream processer with the given operator appended
        to the stream pipeline.

        Parameters
        ----------
        op: openclean.data.stream.processor.StreamOperator
            Stream operator that is appended to the pipeline of the returned
            stream processor.
        columns: list of string, default=None
            Optional (modified) list of column names for the schema of the data
            stream rows.

        Returns
        -------
        openclean.data.stream.processor.StreamProcessor
        """
        return StreamProcessor(
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
        return self.stream(CollectOperator(Count))

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
    ):
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
        openclean.data.stream.processor.StreamProcessor
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
            for rowid, row in self.reader.iterrows():
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
        openclean.data.stream.processor.StreamProcessor
        """
        return self.append(LimitOperator(limit=count))

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
        ds = StreamProcessor(reader=self.reader)
        consumer = self.pipeline[0].open(ds, self.pipeline[1:])
        # Stream all rows an pass them to the consumer.
        for rowid, row in self.reader.iterrows():
            try:
                consumer.consume(rowid, row)
            except StopIteration:
                break
        # Return the result from the consumer when closed at the end of the
        # stream.
        return consumer.close()

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
        return self.append(SelectOperator(columns=list(args)))

    def stream(self, op: StreamOperator):
        """Stream all rows from the associated data file to the data pipeline
        that is associated with this processor. The given operator is appended
        to the current pipeline before execution.

        The returned value is the result that is returned when the consumer is
        generated for the pipeline is closed after processing the data stream.

        Parameters
        -----------
        op: openclean.data.stream.processor.StreamOperator
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

    def update(self, *args):
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
        openclean.data.stream.processor.StreamProcessor
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
