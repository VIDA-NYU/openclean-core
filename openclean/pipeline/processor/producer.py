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
from typing import List, Optional, Tuple

from openclean.data.types import ColumnName, ColumnRef
from openclean.data.select import select_clause
from openclean.data.stream.base import DatasetStream
from openclean.pipeline.consumer.base import StreamConsumer
from openclean.pipeline.consumer.producer import Filter, Limit, Select, Update
from openclean.data.types import Scalar
from openclean.function.eval.base import EvalFunction
from openclean.pipeline.processor.base import StreamProcessor


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
        openclean.data.stream.consumer.StreamConsumer, list of string
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
        openclean.data.stream.consumer.StreamConsumer
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
        openclean.data.stream.consumer.Limit
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
        openclean.data.stream.consumer.Update
        """
        # Get index positions for the updated columns.
        _, colidxs = select_clause(ds, self.columns)
        # Prepate the update funnction
        prep_func = self.func.prepare(ds)
        # Return instance of the update consumer.
        return Update(columns=colidxs, func=prep_func), ds.columns


# -- Data pipeline ------------------------------------------------------------

class DataPipeline(DatasetStream):
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
        self, ds: DatasetStream, columns: Optional[List[ColumnName]] = None,
        pipeline: Optional[StreamProcessor] = None
    ):
        """Initialize the data stream reader, schema information for the
        streamed rows, and the optional
        column filter.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetReader
            Reader for the data stream.
        columns: list of string
            List of column names for the schema of the data stream rows.
        pipeline: list of openclean.data.stream.processor.StreamOperator,
                default=None
            List of operators in the pipeline fpr this stream processor.

        """
        super(DataPipeline, self).__init__(
            columns=columns if columns is not None else ds.columns
        )
        self.reader = ds
        self.pipeline = pipeline if pipeline is not None else list()

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated file handle when the context manager exits."""
        return False

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

    def open(self) -> DataPipeline:
        """

        Returns
        -------
        openclean.data.stream.csv.CSVReader
        """
        return self
