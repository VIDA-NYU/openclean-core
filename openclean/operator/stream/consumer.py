# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract classes defining consumers in a data processing pipeline. Consumers
operate on the rows in a data stream. A consumer may either collect the rows in
a stream (for transformation). Collected rows are then processed or returned
when the consumer is closed at the end of the stream. A consumer can also
manipulate the rows directly and pass them on to a connected downstream consumer.
The latter type of consumer is refered to as a producing consumer. The result
of a producing consumer (that is returned when the consumer is closed at the
end of the stream) is the result that is returned by the connected downstream
consumer.
"""

from __future__ import annotations
from typing import Any, Optional
from abc import ABCMeta, abstractmethod

from openclean.data.stream.base import DataRow, Document, StreamFunction
from openclean.data.types import DatasetSchema


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
    def __init__(self, columns: DatasetSchema):
        """Initialize the schema for rows that this consumer will receive.

        Parameters
        ----------
        columns: list of string
            Names of columns for the rows that the consumer will receive.
        """
        self.columns = columns

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
    def consume(self, rowid: int, row: DataRow) -> DataRow:
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

    def process(self, ds: Document) -> Any:
        """Consume a given data stream and return the computed result.

        Parameters
        ----------
        ds: openclean.data.stream.base.Document
            Iterable stream of dataset rows.

        Returns
        -------
        any
        """
        for rid, row in ds.iterrows():
            self.consume(rowid=rid, row=row)
        return self.close()


class ProducingConsumer(StreamConsumer):
    """A producing consumer passes the processed row on to a downstream
    consumer. This consumer therefore acts as a consumer and a producer.
    """
    def __init__(self, columns: DatasetSchema, consumer: Optional[StreamConsumer]):
        """Initialize the row schema and the optional downstream consumer. Note
        that the consumer is optional for cases where we want to iterate over
        the rows in a stream pipeline.

        Parameters
        ----------
        columns: list of string
            Names of columns for the rows that the consumer will receive.
        consumer: openclean.data.stream.base.StreamConsumer, default=None
            Downstream consumer for processed rows.
        """
        super(ProducingConsumer, self).__init__(columns=columns)
        self.consumer = consumer

    def close(self) -> Any:
        """Return the result of the associated consumer when the end of the
        data stream was reached.

        Returns
        -------
        any
        """
        if self.consumer is not None:
            return self.consumer.close()

    def consume(self, rowid: int, row: DataRow) -> DataRow:
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

        Returns
        -------
        list
        """
        values = self.handle(rowid=rowid, row=row)
        if values is not None:
            if self.consumer is not None:
                return self.consumer.consume(rowid, values)
        return values

    @abstractmethod
    def handle(self, rowid: int, row: DataRow) -> DataRow:
        """Process a given row. Return a modified row or None. In the latter
        case it is assumed that the row should not be passed on to any consumer
        downstream.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.

        Returns
        -------
        list
        """
        raise NotImplementedError()  # pragma: no cover

    def set_consumer(self, consumer: StreamConsumer) -> ProducingConsumer:
        """Set the downstream consumer.

        Parameters
        ----------
        consumer: openclean.data.stream.base.StreamConsumer
            Downstream consumer for processed rows.

        Returns
        -------
        openclean.data.stream.consumer.ProducingConsumer
        """
        self.consumer = consumer
        return self


class StreamFunctionHandler(ProducingConsumer):
    """The stream function handler is a producing consumer that uses an
    associated stream function to handle rows. A stream function should either
    return a modified row or None. Modified rows will be streamed to a connected
    consumer. If None is returned, the row will be ignored.
    """
    def __init__(
        self, columns: DatasetSchema, func: StreamFunction,
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the consumer schema and the stream function that is used
        be the handle method to process rows.

        Parameters
        ----------
        columns: list of string
            Names of columns for the rows that the consumer will receive.
        func: openclean.data.stream.base.StreamFunction
            Stream function used to process data rows in the stream.
        consumer: openclean.data.stream.base.StreamConsumer, default=None
            Downstream consumer for processed rows.
        """
        super(StreamFunctionHandler, self).__init__(
            columns=columns,
            consumer=consumer
        )
        self.func = func

    def handle(self, rowid: int, row: DataRow) -> DataRow:
        """Process a given row using the associated stream function.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.

        Returns
        -------
        list
        """
        return self.func(row)
