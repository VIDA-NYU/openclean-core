# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Consumer for data frame rows in a stream environment."""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from collections import Counter
from typing import Any, List, Optional

import pandas as pd

from openclean.data.column import ColumnName
from openclean.data.stream.csv import CSVWriter
from openclean.function.eval.base import EvalFunction


# -- Abstract base class for data stream consumers ----------------------------

class StreamConsumer(metaclass=ABCMeta):
    """Abstract class for consumers in a data stream processing pipeline. A
    consumer is the instantiation of a StreamOperator that is prepared to
    process (consume) rows in a data stream.

    Each consumer may be is associated with an (optional) downstream consumer
    that will received the processed row from this operator.
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


class ProducingConsumer(StreamConsumer):
    """A producing consumer passes the processed row on to a downstream
    consumer. This consumer therefore acts a s a consumer and a producer.
    """
    def __init__(self, consumer: Optional[StreamConsumer]):
        """Initialize the downstream consumer. Note that the consumer is
        optional for cases where we want to iterate over the rows in a stream
        pipeline.

        Parameters
        ----------
        consumer: openclean.data.stream.consumer.StreamConsumer, default=None
            Downstream consumer for processed rows.
        """
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
        values = self.handle(rowid, row)
        if values is not None:
            if self.consumer is not None:
                return self.consumer.consume(rowid, values)
        return values

    @abstractmethod
    def handle(self, rowid: int, row: List) -> List:
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


# -- Data stream consumers ----------------------------------------------------

class DataFrame(StreamConsumer):
    """Row collector that generates a pandas data frame from the rows in a
    data stream. This consumer will not accept a downstream consumer as it
    would never send any rows to such a consumer.
    """
    def __init__(self, columns: List[ColumnName]):
        """Initialize empty lists for data frame columns, rows and the row
        identifier. These lists will be initialized when the consumer receives
        the open signal.
        """
        self.columns = columns
        self.data = list()
        self.index = list()

    def close(self) -> pd.DataFrame:
        """Closing the consumer yields the data frame with the collected rows.

        Returns
        -------
        ps.DataFrame
        """
        return pd.DataFrame(
            data=self.data,
            columns=self.columns,
            index=self.index
        )

    def consume(self, rowid: int, row: List):
        """Add the row identifier and row values to the respective lists.
        Returns None to avoid that the (empty) downstream consumer is called.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        self.data.append(row)
        self.index.append(rowid)


class Collector(StreamConsumer):
    """The collector is intended primarily for test purposes. Simply collects
    the (rowid, row) pairs that are passed on to it in a list.
    """
    def __init__(self):
        """Initialize the internal buffer."""
        self.rows = list()

    def close(self) -> List:
        """Return the collected row buffer on close.

        Returns
        -------
        list
        """
        return self.rows

    def consume(self, rowid: int, row: List):
        """ Add the given (rowid, row)-pair to the internal buffer. Returns
        the row.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        self.rows.append((rowid, row))


class Distinct(StreamConsumer):
    """Consumer that popuates a counter with the frequency counts for distinct
    values (value combinations) in the processed rows for the data stream.
    """
    def __init__(self):
        """Initialize the counter that maintains the frequency counts for each
        distinct row in the data stream.
        """
        self.counter = Counter()

    def close(self) -> Counter:
        """Closing the consumer yields the populated Counter object.

        Returns
        -------
        collections.Counter
        """
        return self.counter

    def consume(self, rowid: int, row: List):
        """Add the value combination for a given row to the counter. If the
        row contains more than one column a tuple of column values will be
        added to the counter.

        If the row only has one value this value will be used as the key for
        the counter. For rows with multiple values the row will be converted
        to a tuple that is used as the counter key.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        if len(row) == 1:
            self.counter[row[0]] += 1
        else:
            self.counter[tuple(row)] += 1


class Filter(ProducingConsumer):
    """Filter for rows in a data stream. Expects a predicate (a prepared
    evaluation function) to filter rows. Rows that satisfy the given predicate
    are passed on to a given downstream consumer.
    """
    def __init__(
        self, predicate: EvalFunction,
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the predicate that is used to filer rows in the stream
        and the downstream consumer. The predicated is expected to be an
        evaluation function that has been prepared.

        Parameters
        ----------
        predicate: openclean.function.eval.base.EvalFunction
            Evaluation function that is used as the predicate to filter rows
            in the data stream.
        consumer: openclean.data.stream.consumer.StreamConsumer, default=None
            Downstream consumer for rows that satisfy the given predicate.
        """
        super(Filter, self).__init__(consumer)
        self.predicate = predicate

    def handle(self, rowid: int, row: List) -> List:
        """Evaluate the predicate on the given row. Only if the predicate is
        satisfied will the row be passed on to the downstream consumer.

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
        if self.predicate.eval(row):
            return row


class Limit(ProducingConsumer):
    """Consumer that limits the number of rows that are passed on to a
    downstream consumer. Raises a StopIteration error when the maximum number
    of rows is reached.
    """
    def __init__(self, limit: int, consumer: Optional[StreamConsumer] = None):
        """Initialize the row limit and the downstream consumer.

        Parameters
        ----------
        limit: int
            Maximum number of rows that are passed on to the downstream
            consumer.
        consumer: openclean.data.stream.consumer.StreamConsumer, default=None
            Downstream consumer.
        """
        super(Limit, self).__init__(consumer)
        self.limit = limit
        self.count = 0

    def handle(self, rowid: int, row: List) -> List:
        """Pass the row on to the downstream consumer if the row limit has not
        been reached yet. Otherwise, a StopIteration error is raised.

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
        if self.count < self.limit:
            self.count += 1
            return row
        else:
            raise StopIteration()


class Select(ProducingConsumer):
    """Filter for data stream columns. Allows to remove (and reorder) column
    values in data stream rows.
    """
    def __init__(
        self, columns: List[int], consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the list of columns that are included in the output
        data stream rows from this consumer and the downstream consumer.

        Parameters
        ----------
        columns: list of int
            List of column index positions for the columns that are included in
            the processed rows.
        consumer: openclean.data.stream.consumer.StreamConsumer, default=None
            Downstream consumer.
        """
        super(Select, self).__init__(consumer)
        self.columns = columns

    def handle(self, rowid: int, row: List) -> List:
        """Return a row that only contains the columns in the selected list.

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
        return [row[i] for i in self.columns]


class Write(StreamConsumer):
    """Write data stream rows to an output file."""
    def __init__(self, writer: CSVWriter):
        """Initialize the CSV writer.s

        Parameters
        ----------
        writer: openclean.data.stream.csv.CSVWriter
            Writer for rows in a CSV file.
        """
        self.writer = writer

    def close(self):
        """Close the associated CSV writer when the end of the data stream was
        reached.
        """
        self.writer.close()

    def consume(self, rowid: int, row: List):
        """Write the row values to the output file.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        self.writer.write(row)
