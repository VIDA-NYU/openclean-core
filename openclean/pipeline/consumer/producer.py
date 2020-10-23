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
from openclean.operator.stream.consumer import StreamConsumer


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
        consumer: openclean.data.stream.base.StreamConsumer, default=None
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
        consumer: openclean.data.stream.base.StreamConsumer, default=None
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
