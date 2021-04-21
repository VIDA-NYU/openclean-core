# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Implementation of stream operators that collect a random sampe of rows from
a data stream.
"""

from random import Random
from typing import Any, Optional

from openclean.data.stream.base import DataRow
from openclean.data.types import DatasetSchema
from openclean.operator.stream.consumer import ProducingConsumer, StreamConsumer
from openclean.operator.stream.processor import StreamProcessor


class Sample(StreamProcessor):
    """Processor that defines a random sampling operator in a data stream.
    """
    def __init__(self, n: int, random_state: Optional[int] = None):
        """Initialize the sample size and the optional seed for the random
        number generator.

        ----------
        n: int
            Size of the collected random sample.
        random_state: int, default=None
            Seed value for the random number generator (for reproducibility
            purposes).
        """
        self.n = n
        self.random_state = random_state

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        random sample generator in a data pipeline.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer
        """
        return SampleCollector(columns=schema, n=self.n, random_state=self.random_state)


class SampleCollector(ProducingConsumer):
    """Collect a random sample of rows from the data stream and pass them to
    a downstream consumer. Implements random sampling without replacement using
    the Reservoir Sampling algorithm. See:

    Alon N., Matias Y., and Szegedy M.
    The space complexity of approximating the frequency moments.
    J. Comput. Syst. Sci., 58(1):137–147, 1999.

    and

    Lahiri B., Tirthapura S. (2009)
    Stream Sampling.
    In: LIU L., ÖZSU M.T. (eds) Encyclopedia of Database Systems.
    Springer, Boston, MA. https://doi.org/10.1007/978-0-387-39940-9_372

    for details.

    Maintains a row buffer with n rows. Pushes the final row set to the
    downstream consumer at the end of the stream.
    """
    def __init__(
        self, columns: DatasetSchema, n: int, random_state: Optional[int] = None,
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the row schema, sample size and the internal row buffer.
        Provides the option to seed the random number generator for
        reproducibility.

        ----------
        n: int
            Size of the collected random sample.
        random_state: int, default=None
            Seed value for the random number generator (for reproducibility
            purposes).
        columns: list of string
            Names of columns for the rows that the consumer will receive.
        consumer: openclean.data.stream.base.StreamConsumer, default=None
            Downstream consumer for processed rows.
        """
        super(SampleCollector, self).__init__(columns=columns, consumer=consumer)
        self.size = n
        # Initialize the random number generator.
        self.rand = Random()
        if random_state is not None:
            self.rand.seed(random_state)
        # Initialize the row buffer for the selected rows, (row-id, row)-pairs.
        self.rows = list()
        # conter for the total number of rows received.
        self.count = 0

    def close(self) -> Any:
        """Pass the selected sample to the connected downstream consumer.
        Returns the consumer result.

        Collect a modified list of rows. Returns the result of the downstream
        consumer or the collected results (if the consumer result is None).

        Returns
        -------
        any
        """
        if self.consumer is not None:
            result = list()
            for rowid, row in self.rows:
                try:
                    row = self.consumer.consume(rowid=rowid, row=row)
                    if row is not None:
                        result.append((rowid, row))
                except StopIteration:
                    break
            consumer_result = self.consumer.close()
            return result if consumer_result is None else consumer_result
        else:
            return self.rows

    def consume(self, rowid: int, row: DataRow):
        """Randomly add the given (rowid, row)-pair to the internal buffer.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        self.handle(rowid=rowid, row=row)

    def handle(self, rowid: int, row: DataRow):
        """Add the given row to the buffer if the maximum buffer size has not
        been reached yet or the row is randomly selected for inclusion in the
        sample.

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
        self.count += 1
        if self.count <= self.size:
            self.rows.append((rowid, row))
        else:
            # Include the element with probability n/(t+1), i.e,, size/count.
            p = self.size / self.count
            if self.rand.random() <= p:
                # Randomly select element for replacement.
                index = self.rand.randrange(self.size)
                self.rows[index] = (rowid, row)
