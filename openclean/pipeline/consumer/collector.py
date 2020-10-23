# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collectors are data stream consumers that are not connected to a downstream
consumer. These consumers collect results until the close method of the data
stream is called. At that point, the collector returns a final results.

This module contains imlementations for various basic collectors.
"""

from collections import Counter
from typing import List

import pandas as pd

from openclean.data.types import ColumnName
from openclean.data.stream.csv import CSVWriter
from openclean.operator.stream.consumer import StreamConsumer


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
        """Add the given (rowid, row)-pair to the internal buffer. Returns
        the row.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        self.rows.append((rowid, row))


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


class Distinct(StreamConsumer):
    """Consumer that popuates a counter with the frequency counts for distinct
    values (or value combinations) in the processed rows for the data stream.
    """
    def __init__(self):
        """Initialize the counter that maintains the frequency counts for each
        distinct row in the data stream.
        """
        self.counter = Counter()

    def close(self) -> Counter:
        """Closing the consumer returns the populated Counter object.

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


class RowCount(StreamConsumer):
    """The row counter is a simple counter for the number of (rowid, row) pairs
    that are passed on to consumer.
    """
    def __init__(self):
        """Initialize the internal row counter."""
        self.rows = 0

    def close(self) -> int:
        """Return the couter value.

        Returns
        -------
        int
        """
        return self.rows

    def consume(self, rowid: int, row: List):
        """Increament the counter value.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        self.rows += 1


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
