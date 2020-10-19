# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Producers in a data stream are consumers that pass (processed) rows in a
data stream on to a connected downstream consumer.
"""

from __future__ import annotations
from abc import abstractmethod
from typing import Any, List, Optional

from openclean.data.types import Scalar
from openclean.function.eval.base import EvalFunction
from openclean.pipeline.consumer.base import StreamConsumer


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
        consumer: openclean.pipeline.consumer.base.StreamConsumer, default=None
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


class Filter(ProducingConsumer):
    """Filter for rows in a data stream. Expects a predicate (a prepared
    evaluation function) to filter rows. Rows that satisfy the given predicate
    are passed on to a given downstream consumer.
    """
    def __init__(
        self, predicate: EvalFunction, truth_value: Optional[Scalar] = True,
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
        truth_value: scalar, defaut=True
            Return value of the predicate that signals that the predicate is
            satisfied by an input value.
        consumer: openclean.pipeline.consumer.base.StreamConsumer, default=None
            Downstream consumer for rows that satisfy the given predicate.
        """
        super(Filter, self).__init__(consumer)
        self.predicate = predicate
        self.truth_value = truth_value

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
        if self.predicate.eval(row) == self.truth_value:
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
        consumer: openclean.pipeline.consumer.base.StreamConsumer, default=None
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
        consumer: openclean.pipeline.consumer.base.StreamConsumer, default=None
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


class Update(ProducingConsumer):
    """Update operator for rows in a data stream. Expects a list of columns
    and an update function. Updated rows are passed on to a given downstream
    consumer.
    """
    def __init__(
        self, columns: List[int], func: EvalFunction,
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the columns that are updated and the update function.
        Columns are referenced by their index in the schema. The update
        function is expected to be an evaluation function. The function as is
        already prepared.

        Parameters
        ----------
        columns: list of int
            List of column index positions for the columns that are updated.
        func: openclean.function.eval.base.EvalFunction
            Evaluation function that is used to generate values for the updated
            columns in each row of the data stream.
        consumer: openclean.pipeline.consumer.base.StreamConsumer, default=None
            Downstream consumer for updated rows.
        """
        super(Update, self).__init__(consumer)
        self.columns = columns
        self.func = func

    def handle(self, rowid: int, row: List) -> List:
        """Update rows and return the updated result.

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
        val = self.func.eval(row)
        values = list(row)
        if len(self.columns) == 1:
            values[self.columns[0]] = val
        else:
            if len(val) != len(self.columns):
                msg = 'expected {} values instead of {}'
                raise ValueError(msg.format(len(self.columns), len(val)))
            for i, col in enumerate(self.columns):
                values[col] = val[i]
        return values
