# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data frame transformer (and stream processor) that can be used to limit the
number of rows in a data frame. The limit operator is primarily intended for
use in streaming settings. The data frame transformer implementation is included
for completeness.
"""

from typing import Optional

import pandas as pd

from openclean.data.stream.base import DataRow
from openclean.data.types import DatasetSchema
from openclean.operator.base import DataFrameTransformer
from openclean.operator.stream.consumer import StreamConsumer, ProducingConsumer
from openclean.operator.stream.processor import StreamProcessor


# -- Functions ----------------------------------------------------------------

def limit(df: pd.DataFrame, rows: int) -> pd.DataFrame:
    """Limit the number of rows in a data frame. Returns a data frame that
    contains at most the first n (n=rows) rows from the input data frame.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame.
    rows: int
        Limit on number of rows in the result. Rows are included starting from
        the first row until either the row limit or end of the data frame is
        reached (whatever comes first).

    Returns
    -------
    pd.DataFrame
    """
    return Limit(rows=rows).transform(df=df)


# -- Operators ----------------------------------------------------------------

class Limit(StreamProcessor, DataFrameTransformer):
    """Transformer and stream processor that limits the number of rows in a
    data frame.
    """
    def __init__(self, rows: int):
        """Initialize the row limit.

        Parameters
        ----------
        rows: int
            Limit on number of rows in any data frame that is processed by this
            operator. Rows are processed starting from the first row until
            either the row limit or end of the data frame is reached (whatever
            comes first).
        """
        self.rows = rows

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of a
        stream consumer that limits the number of rows that are passed on to a
        downstream consumer.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.transformer.limit.LimitConsumer
        """
        return LimitConsumer(columns=schema, limit=self.rows)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a data frame that contains at most n of rows (where n equals
        the row limit that was set when this object was created).

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        pd.DataFrame
        """
        return df.head(n=self.rows)


class LimitConsumer(ProducingConsumer):
    """Consumer that limits the number of rows that are passed on to a
    downstream consumer. Raises a StopIteration error when the maximum number
    of rows is reached.
    """
    def __init__(
        self, columns: DatasetSchema, limit: int,
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the row limit and the downstream consumer.

        Parameters
        ----------
        columns: list of string
            Names of columns for the rows that the consumer will receive.
        limit: int
            Maximum number of rows that are passed on to the downstream
            consumer.
        consumer: openclean.data.stream.base.StreamConsumer, default=None
            Downstream consumer.
        """
        super(LimitConsumer, self).__init__(columns=columns, consumer=consumer)
        self.limit = limit
        self.count = 0

    def handle(self, rowid: int, row: DataRow) -> DataRow:
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
