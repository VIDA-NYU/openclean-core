# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for data stream processing pipelines."""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Any, Callable, Iterator, List, Tuple

from openclean.data.types import ColumnName, Scalar, Value


# -- Data stream consumer -----------------------------------------------------

class StreamConsumer(metaclass=ABCMeta):
    """Abstract class for consumers in a data stream processing pipeline. A
    consumer is the instantiation of a StreamProcessor that is prepared to
    process (consume) rows in a data stream.

    Each consumer may be is associated with an (optional) downstream consumer
    that will received the processed row from this operator. Consumers that
    are connected to a downstream consumer are also refered to as producers.
    Consumers that are not connected to a downstream consumer are called
    collectors. There are separate modules for each type of consumers.
    """
    @abstractmethod
    def close(self) -> Any:
        """Signal that the end of the data stream has reached. The return value
        is implementation dependent.

        Returns
        -------
        any
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def consume(self, rowid: int, row: List) -> List:
        """Consume the given row. Passes the processed row on to an associated
        downstream consumer. Returns the processed row. If the result is None
        this signals to a collector/iterator that the given row should not be
        part of the collected/yielded result.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        raise NotImplementedError()  # pragma: no cover

    def process(self, ds: DatasetStream) -> Any:
        """Consume a given data stream and return the computed result.

        Parameters
        ----------
        ds: from openclean.data.stream.base.DatasetStream
            Iterable stream of dataset rows.

        Returns
        -------
        any
        """
        for rid, row in ds.iterrows():
            self.consume(rowid=rid, row=row)
        return self.close()


"""Type alias for stream functions. Stream functions are callables that
accept a data frame row as the only argument. They return a Value.
"""
DataRow = List[Scalar]
StreamFunction = Callable[[DataRow], Value]


# _- Data frame readers and writers -------------------------------------------

class DatasetIterator(metaclass=ABCMeta):
    """Abstract class for iterators over rows in a data frame. Data frame
    iterators are also context managers and iterators. Therefore, in addition
    to the header method, implementations are expected to implement (i) the
    __enter__ and __exit__ methods for a context manager, and (ii) the __iter__
    and __next__ method for Python iterators.
    """


class DatasetStream(metaclass=ABCMeta):
    """Reader for data streams. Provides the functionality to open the stream
    for reading. Dataset reader should be able to read the same dataset
    multiple times.
    """
    def __init__(self, columns: List[ColumnName]):
        """Initialize the schema for the rows in this data stream iterator.

        Parameters
        ----------
        columns: list of string
            Schema for data stream rows.
        """
        self.columns = columns

    def iterrows(self) -> Iterator[Tuple[int, List]]:
        """Simulate the iterrows() function of a pandas DataFrame as it is used
        in openclean. Returns an iterator that yields pairs of row identifier
        and value list for each row in the streamed data frame.

        Returns
        -------
        iterator
        """
        with self.open() as f:
            for rowid, row in f:
                yield rowid, row

    @abstractmethod
    def open(self) -> DatasetIterator:
        """Open the data stream to get a iterator for the rows in the dataset.

        Returns
        -------
        openclean.data.stream.base.DatasetIterator
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def stream(self, consumer: StreamConsumer) -> Any:
        """Stream all rows to a given stream consumer. Closes the consumer at
        the end of the stream and returns the result.

        The consumer may raise a StopIteration error to signal that it will not
        accept any further row. An implementation of the dataset stream should
        catch the error and close the consumer to return its result.

        Parameters
        ----------
        consumer: openclean.data.stream.base.StreamConsumer
            Consumer for dataset rows.

        Returns
        -------
        any
        """
        raise NotImplementedError()
