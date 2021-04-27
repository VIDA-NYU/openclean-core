# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data pipeline for processing datasets as streams of rows."""

from __future__ import annotations
from collections import Counter
from typing import Callable, Dict, List, Optional, Tuple, Type, Union

import pandas as pd

from openclean.data.mapping import Mapping
from openclean.data.schema import as_list, select_clause
from openclean.data.stream.base import DataRow, Datasource, DefaultDocument, DocumentIterator, RowIndex, to_document
from openclean.data.stream.csv import CSVFile
from openclean.data.stream.df import DataFrameStream
from openclean.data.types import Columns, Scalar, DatasetSchema, Value
from openclean.cluster.base import Cluster, Clusterer
from openclean.function.eval.base import EvalFunction
from openclean.function.matching.base import StringMatcher
from openclean.operator.stream.collector import DataFrame, Distinct, RowCount, Write
from openclean.operator.stream.consumer import StreamConsumer
from openclean.operator.stream.matching import BestMatches
from openclean.operator.stream.processor import StreamProcessor
from openclean.operator.stream.sample import Sample
from openclean.operator.transform.filter import Filter
from openclean.operator.transform.insert import InsCol
from openclean.operator.transform.limit import Limit
from openclean.operator.transform.move import MoveCols
from openclean.operator.transform.rename import Rename
from openclean.operator.transform.select import Select
from openclean.operator.transform.update import Update, UpdateFunction
from openclean.profiling.dataset import ColumnProfiler, ProfileOperator
from openclean.profiling.datatype.convert import DatatypeConverter
from openclean.profiling.datatype.operator import Typecast


class DataPipeline(DefaultDocument):
    """The data pipeline allows to iterate over the rows that are the result of
    streaming an input data set through a pipeline of stream operators.

    The class implements the context manager interface.
    """
    def __init__(
        self, source: Datasource, columns: Optional[DatasetSchema] = None,
        pipeline: Optional[StreamProcessor] = None
    ):
        """Initialize the data stream reader, schema information for the
        streamed rows, and the optional pipeline operators.

        Parameters
        ----------
        source: openclean.data.stream.base.Datasource
            Reader for the data stream.
        columns: list of string, default=None
            List of column names in the schema of the pipeline result.
        pipeline: list of openclean.data.stream.processor.StreamProcessor,
                default=None
            List of operators in the pipeline fpr this stream processor.

        """
        # Ensure that the source document is an instance of the class
        # histore.document.base.Document.
        self.source = to_document(source)
        super(DataPipeline, self).__init__(
            columns=columns if columns is not None else source.columns
        )
        self.pipeline = pipeline if pipeline is not None else list()

    def __enter__(self):
        """Enter method for the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Close the associated document when the context manager exits."""
        self.close()
        return False

    def append(
        self, op: StreamProcessor, columns: Optional[DatasetSchema] = None
    ) -> DataPipeline:
        """Return a modified stream processer with the given operator appended
        to the stream pipeline.

        Parameters
        ----------
        op: openclean.operator.stream.processor.StreamProcessor
            Stream operator that is appended to the pipeline of the returned
            stream processor.
        columns: list of string, default=None
            Optional (modified) list of column names for the schema of the data
            stream rows.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        return DataPipeline(
            source=self.source,
            columns=columns if columns is not None else self.columns,
            pipeline=self.pipeline + [op]
        )

    def close(self):
        """Close the associated document."""
        self.source.close()

    def cluster(self, clusterer: Clusterer) -> List[Cluster]:
        """Cluster values in a data stream.

        This operator will create a distinct set of values in the data stream
        rows. The collected values are then passed on to the given cluster
        algorithm.

        Parameters
        ----------
        clusterer: openclean.cluster.base.Clusterer
            Cluster algorithm for distinct values in the data stream.

        Returns
        -------
        list of openclean.cluster.base.Cluster
        """
        return self.stream(clusterer)

    def count(self) -> int:
        """Count the number of rows in a data stream.

        Returns
        -------
        int
        """
        return self.stream(RowCount())

    def delete(self, predicate: EvalFunction) -> DataPipeline:
        """Remove rows from the data stream that satisfy a given condition.

        Parameters
        ----------
        predicate: opencelan.function.eval.base.EvalFunction
            Evaluation function used to delete rows.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        # Create a new stream processor with a negated filter operator appended
        # to the pipeline to remove rows from the stream.
        return self.append(Filter(predicate=predicate, negated=True))

    def distinct(self, columns: Optional[Columns] = None) -> Counter:
        """Get counts for all distinct values over all columns in the
        associated data stream. Allows the user to specify the list of columns
        for which they want to count values.

        Parameters
        ----------
        columns: int, str, or list of int or string, default=None
            References to the column(s) for which unique values are counted.

        Returns
        -------
        collections.Counter
        """
        return self.stream(Distinct(columns=columns))

    def distinct_values(self, columns: Optional[Columns] = None) -> List[Value]:
        """Get list all distinct values over all columns in the associated data
        stream.

        Provides the option to the user to specify the list of columns for
        which they want to count values.

        Parameters
        ----------
        columns: int, str, or list of int or string, default=None
            References to the column(s) for which unique values are counted.

        Returns
        -------
        collections.Counter
        """
        return list(self.distinct(columns=columns).keys())

    def filter(
        self, predicate: EvalFunction, limit: Optional[int] = None
    ) -> DataPipeline:
        """Filter rows in the data stream that satisfy a given condition.
        Allows to limit the number of rows in the returned data stream.

        Parameters
        ----------
        predicate: opencelan.function.eval.base.EvalFunction
            Evaluation function used to filter rows.
        limit: int, default=None
            Limit the number of rows in the filtered data stream.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        # Create a new stream processor with a filter operator appended to the
        # pipeline.
        op = Filter(predicate=predicate)
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
            consumer = self._open_pipeline()
            for rowid, row in self.source.iterrows():
                try:
                    row = consumer.consume(rowid, row)
                    if row is not None:
                        yield rowid, row
                except StopIteration:
                    break
            consumer.close()
        else:
            for rowid, row in self.source.iterrows():
                yield rowid, row

    def insert(
        self, names: DatasetSchema, pos: Optional[int] = None,
        values: Optional[Union[Callable, Scalar, EvalFunction, List, Tuple]] = None
    ) -> DataPipeline:
        """Insert one or more columns into the rows in the data stream.

        Parameters
        ----------
        names: string, or list(string)
            Names of the inserted columns.
        pos: int, optional
            Insert position for the new columns. If None the columns will be
            appended.
        values: scalar,
                list,
                callable, or
                openclean.function.eval.base.EvalFunction, optional
            Single value, list of constant values, callable that accepts a data
            frame row as the only argument and returns a (list of) value(s)
            matching the number of columns inserted or an evaluation function
            that returns a matchin number of values.
        """
        op = InsCol(names=names, pos=pos, values=values)
        inspos = op.inspos(self.columns)
        columns = self.columns[:inspos] + as_list(names) + self.columns[inspos:]

        return self.append(op=op, columns=columns)

    def limit(self, count: int) -> DataPipeline:
        """Return a data stream for the data frame that will yield at most
        the first n rows passed to it from an associated producer.

        Parameters
        ----------
        count: int
            Maximum number of rows in the returned data frame.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        return self.append(Limit(rows=count))

    def match(
        self, matcher: StringMatcher, include_vocab: Optional[bool] = False
    ) -> Mapping:
        """Generate a mapping of best matches between a given vocabulary and
        the values in one (or more) column(s) of the data stream. For each row
        (i.e., single value) the best matches with a given vocabulary is
        computed and added to the returned mapping.

        For rows that contain multiple columns an error will be raised.

        If the include_vocab flag is False the resulting mapping will contain a
        mapping only for those values that do not occur in the vocabulary,
        i.e., the unknown values with respect to the vocabulary.

        Parameters
        ----------
        matcher: openclean.function.matching.base.VocabularyMatcher
            Matcher to compute matches for the terms in a controlled vocabulary.
        include_vocab: bool, default=False
            If this flag is False the resulting mapping will only contain matches
            for terms that are not in the vocabulary that is associated with the
            given matcher.

        Returns
        -------
        openclean.data.mapping.Mapping
        """
        collector = BestMatches(matcher=matcher, include_vocab=include_vocab)
        return self.stream(collector)

    def move(self, columns: Columns, pos: int) -> DataPipeline:
        """Move one or more columns in a data stream schema.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        pos: int
            Insert position for the moved columns.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        op = MoveCols(columns=columns, pos=pos)
        colorder = op.reorder(self.columns)
        sortcols = [self.columns[i] for i in colorder]
        return self.append(op=op, columns=sortcols)

    def open(self) -> DataPipeline:
        """Return reference to self when the pipeline is opened.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        # Create the consumer if the pipeline has at least one operator.
        consumer = None
        if self.pipeline:
            consumer = self._open_pipeline()
        # Stream all rows to the pipeline consumer.
        return PipelineIterator(stream=self.source.open(), consumer=consumer)

    def _open_pipeline(self) -> StreamConsumer:
        """Create stream consumer for all pipeline operators.

        Connect the created operators to ensure that rows are passed through the
        pipeline. Returns a reference to the consumer for the first operator.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer
        """
        # Create a stream consumer for the first operator in the pipeline. This
        # consumer is the one that will receive all dataset rows first.
        pipeline = self.pipeline[0].open(schema=self.source.columns)
        # Create consumer for downstream operators and connect the consumer
        # with each other. This assumes that all operaotrs (except the last
        # one) yield consumer that are also producer.
        producer = pipeline
        for op in self.pipeline[1:]:
            consumer = op.open(producer.columns)
            producer.set_consumer(consumer)
            producer = consumer
        return pipeline

    def persist(self, filename: Optional[str] = None) -> DataPipeline:
        """Persist the results of the current stream for future processing.
        The data can either be written to disk or persitet in a in-memory
        data frame (depending on whether a filename is specified).

        The persist operator is currently not lazzily evaluated.

        Parameters
        ----------
        filename: string, default=None
            Path to file on disk for storing the pipeline result. If None, the
            data is persistet in-memory as a pandas data frame.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        if filename is not None:
            # Write current pipeline result to disk and return a stream to the
            # data file.
            self.write(filename)
        else:
            # Create an in-memory data frame and assignt it to the filename
            # parameter. The stream function will return a data frame stream
            # based on the argument type.
            filename = self.to_df()
        return stream(filename)

    def profile(
        self, profilers: Optional[ColumnProfiler] = None,
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
                and openclean.profiling.base.DataProfiler, default=None
            Specify the list of columns that are profiled and the profiling
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
        op = ProfileOperator(
            profilers=profilers,
            default_profiler=default_profiler
        )
        return self.stream(op)

    def rename(self, columns: Columns, names: DatasetSchema) -> DataPipeline:
        """Rename selected columns in a the schema data of data stream rows.

        Parameters
        ----------
        columns: int, str, or list of int or string
            References to renamed columns.
        names: int, str, or list of int or string, default=None
            New names for the selected columns.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        op = Rename(columns=columns, names=names)
        return self.append(op=op, columns=op.rename(self.columns))

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
        # any consumer that could generate a result.
        if not self.pipeline:
            return None
        # Create a stream consumer for the first operator in the pipeline. This
        # consumer is the one that will receive all dataset rows first.
        consumer = self._open_pipeline()
        # Stream all rows to the pipeline consumer. The returned result is the
        # result that is returned when the consumer is closed by the reader.
        with self.source.open() as stream:
            for _, rowid, row in stream:
                try:
                    consumer.consume(rowid=rowid, row=row)
                except StopIteration:
                    break
        return consumer.close()

    def sample(self, n: int, random_state: Optional[int] = None) -> DataPipeline:
        """Add operator for a random sample generator to the data stream.

        ----------
        n: int
            Size of the collected random sample.
        random_state: int, default=None
            Seed value for the random number generator (for reproducibility
            purposes).
        """
        return self.append(Sample(n=n, random_state=random_state))

    def select(
        self, columns: Optional[Columns] = None, names: Optional[DatasetSchema] = None
    ) -> DataPipeline:
        """Select a given list of columns from the streamed data frame. Columns
        in the resulting data stream may also be renamed using the optional
        list of new column names.

        Returns a new data stream with the column filter set to the columns
        that were in the argument list.

        Parameters
        ----------
        columns: int, str, or list of int or string, default=None
            References to the selected columns.
        names: int, str, or list of int or string, default=None
            Optional renaming for selected columns.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        # Use the full data stream schema if no column list is given.
        if columns is None:
            columns = list(range(len(self.columns)))
        # Select the columns first.
        op = Select(columns=columns)
        colnames, _ = select_clause(schema=self.columns, columns=columns)
        ds = self.append(op=Select(columns=columns), columns=colnames)
        # Append an optional column rename operator if new column names are
        # given. New column names are expected to be in the same order as the
        # selected columns.
        if names is not None:
            op = Rename(columns=list(range(len(columns))), names=names)
            ds = ds.append(op=op, columns=names)
        return ds

    def stream(self, op: StreamProcessor):
        """Stream all rows from the associated data file to the data pipeline
        that is associated with this processor. The given operator is appended
        to the current pipeline before execution.

        The returned value is the result that is returned when the consumer is
        generated for the pipeline is closed after processing the data stream.

        Parameters
        -----------
        op: openclean.operator.stream.processor.StreamProcessor
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
        return self.stream(DataFrame())

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
        return self.append(Typecast(converter=converter))

    def update(self, columns: Columns, func: UpdateFunction) -> DataPipeline:
        """Update rows in a data frame.

        Raises a Value error if not enough arguments (at least two) are given.

        Parameters
        ----------
        columns: int, str, or list of int or string, default=None
            References to the selected columns.
        func: scalar, dict, callable, openclean.function.value.base.ValueFunction,
            or openclean.function.eval.base.EvalFunction
            Specification of the (resulting) evaluation function that is used to
            generate the updated values for each row in the data frame.

        Returns
        -------
        openclean.data.stream.processor.StreamProcessor
        """
        return self.append(Update(columns=columns, func=func))

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
        openclean.pipeline.DataPipeline
        """
        return self.filter(predicate=predicate, limit=limit)

    def write(
        self, filename: str, delim: Optional[str] = None,
        compressed: Optional[bool] = None, none_as: Optional[str] = None,
        encoding: Optional[str] = None
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
        none_as: string, default=None
            String that is used to encode None values in the output file. If
            given, all cell values that are None are substituted by the string.
        encoding: string, default=None
            The csv file encoding e.g. utf-8, utf-16 etc.
        """
        file = CSVFile(
            filename=filename,
            delim=delim,
            compressed=compressed,
            encoding=encoding,
            none_is=none_as
        )
        return self.stream(Write(file=file))


class PipelineIterator(DocumentIterator):
    """Iterator over rows in a data processing pipeline. Iterates over the rows
    in an input stream. Each row is processed by a stream consumer. If the
    consumer returns a value this value is returned as the next row. For
    consumers that only return a result at the end of the stream this iterator
    iterates over the rows that are returned when the consumer is closed.
    """
    def __init__(self, stream: DocumentIterator, consumer: Optional[StreamConsumer] = None):
        """Initialize the source stream and the data processor.

        Keeps an internal list of rows that may be returned by the stream
        consumer when it is closed.

        If the consumer is None the rows of the input stream will be returned
        by this iterator.

        Parameters
        ----------
        stream: openclean.data.stream.base.DocumentIterator
            Iterator over the rows in the input document.
        consumer: openclean.operator.stream.consumer.StreamConsumer, default=None
            Processor for stream rows.
        """
        self.stream = stream
        self.consumer = consumer
        # Maintain reader for rows that may be returned by the consumer when
        # it is closed.
        self._rows = None
        self._readindex = None
        # Counter for returned rows.
        self._rowcount = 0

    def close(self):
        """Release all resources that are held by the associated input stream
        and output consumer.
        """
        self.stream.close()

    def next(self) -> Tuple[int, RowIndex, DataRow]:
        """Read the next row in the pipeline.

        The row is processed by the associated consumer. If the consumer returns
        an non-None result this row is returned as the next row. If the consumer
        returns None the next input row is processed. If the consumer returns a
        non-empty result when it is closed we assume that this is a list of rows
        and iterate over them as well.

        Returns
        -------
        tuple of int, histore.document.base.RowIndex, histore.document.base.DataRow
        """
        # If the row-buffer is not None return rows from the buffer.
        if self._rows is not None:
            if self._readindex < len(self._rows):
                pos = self._rowcount
                self._rowcount += 1
                rowidx, row = self._rows[self._readindex]
                self._readindex += 1
                return pos, rowidx, row
            raise StopIteration()
        # Process rows in the input stream. Returns at the first row that is
        # processed by the consumer with a non-None result. The stream reader
        # will raise StopIteration when the end of the stream is reached. At
        # that point the loop will terminate.
        while True:
            try:
                pos, rowid, row = self.stream.next()
                if self.consumer is not None:
                    row = self.consumer.consume(rowid=rowid, row=row)
                if row is not None:
                    self._rowcount += 1
                    return pos, rowid, row
            except StopIteration:
                break
        # Close the consumer. If it returns a non-None result we assume that
        # it is a list of rows (rowid, values) and continue to iterate over
        # them.
        if self.consumer is not None:
            self._rows = self.consumer.close()
            if self._rows is not None:
                pos = self._rowcount
                self._rowcount += 1
                self._readindex = 1
                rowidx, row = self._rows[0]
                return pos, rowidx, row
        raise StopIteration()


# -- Open file or data frame as pipeline --------------------------------------

def stream(
    filename: Union[str, pd.DataFrame], header: Optional[DatasetSchema] = None,
    delim: Optional[str] = None, compressed: Optional[bool] = None,
    none_is: Optional[str] = None, encoding: Optional[str] = None
) -> DataPipeline:
    """Read a CSV file as a data stream. This is a helper method that is
    intended to read and filter large CSV files.

    Parameters
    ----------
    filename: string
        Path to CSV file on the local file system.
    header: list of string, default=None
        Optional header. If no header is given it is assumed that the first
        row in the CSV file contains the header information.
    delim: string, default=None
        The column delimiter used in the CSV file.
    compressed: bool, default=None
        Flag indicating if the file contents have been compressed using
        gzip.
    none_is: string, default=None
        String that was used to encode None values in the input file. If
        given, all cell values that match the given string are substituted
        by None.
    encoding: string, default=None
        The csv file encoding e.g. utf-8, utf16 etc

    Returns
    -------
    openclean.pipeline.DataPipeline
    """
    if isinstance(filename, pd.DataFrame):
        file = DataFrameStream(df=filename)
    else:
        file = CSVFile(
            filename=filename,
            header=header,
            delim=delim,
            compressed=compressed,
            none_is=none_is,
            encoding=encoding
        )
    return DataPipeline(source=file)
