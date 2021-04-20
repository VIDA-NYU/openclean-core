# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collectors are data stream consumers that are not connected to a downstream
consumer. These consumers collect results until the close method of the data
stream is called. At that point, the collector returns a final results.

This module contains implementations of the StreamConsumer and StremProcessor
interface for various basic collectors.
"""

from collections import Counter
from typing import List, Optional

import pandas as pd

from openclean.data.schema import as_list, select_clause
from openclean.data.types import Columns, DatasetSchema
from openclean.data.stream.csv import CSVFile, CSVWriter
from openclean.operator.stream.consumer import StreamConsumer
from openclean.operator.stream.processor import StreamProcessor


# -- Row Collector ------------------------------------------------------------

class Collector(StreamConsumer, StreamProcessor):
    """The collector is intended primarily for test purposes. Simply collects
    the (rowid, row) pairs that are passed on to it in a list.
    """
    def __init__(self):
        """Initialize the row schema and the internal buffer."""
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

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        stream consumer for the row collector.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer
        """
        return Collector()


class DataFrame(StreamConsumer, StreamProcessor):
    """Row collector that generates a pandas data frame from the rows in a
    data stream. This consumer will not accept a downstream consumer as it
    would never send any rows to such a consumer.
    """
    def __init__(self, columns: Optional[DatasetSchema] = None):
        """Initialize empty lists for data frame columns, rows and the row
        identifier. These lists will be initialized when the consumer receives
        the open signal.

        Parameters
        ----------
        columns: list of string
            Column names for the generated data frame. The columns are only
            given if the operator is instantiated as a consumer.
        """
        self.columns = columns
        self.data = list()
        self.index = list()
        self.dtypes = object

    def close(self) -> pd.DataFrame:
        """Closing the consumer yields the data frame with the collected rows.

        Returns
        -------
        ps.DataFrame
        """
        return pd.DataFrame(
            data=self.data,
            columns=self.columns,
            index=self.index,
            dtype=self.dtypes
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

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        stream consumer for the data frame generator.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer
        """
        return DataFrame(columns=schema)


class Distinct(StreamConsumer, StreamProcessor):
    """Consumer that popuates a counter with the frequency counts for distinct
    values (or value combinations) in the processed rows for the data stream.
    """
    def __init__(self, columns: Optional[Columns] = None):
        """Initialize the counter that maintains the frequency counts for each
        distinct row in the data stream.
        """
        self.counter = Counter()
        self.columns = columns

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
        if len(self.columns) == 1:
            self.counter[row[self.columns[0]]] += 1
        else:
            self.counter[tuple([row[i] for i in self.columns])] += 1

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        stream consumer for the distinct values collector.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer
        """
        columns = self.columns if self.columns else schema
        _, colidx = select_clause(schema, columns=as_list(columns))
        return Distinct(columns=colidx)


class RowCount(StreamConsumer, StreamProcessor):
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

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        stream consumer for the row counter.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer
        """
        return RowCount()


class Write(StreamConsumer, StreamProcessor):
    """Write data stream rows to an output file. This class either contains a
    reference to a CSV file (if instantiated as a processor) or a reference to
    a CSV writer (if instantiated as a consumer).
    """
    def __init__(self, file: Optional[CSVFile] = None, writer: Optional[CSVWriter] = None):
        """Initialize the CSV file and the CSV writer.

        Parameters
        ----------
        file: openclean.data.stream.csv.CSVFile
            Reference to the output CSV file.
        writer: openclean.data.stream.csv.CSVWriter
            Writer for the output CSV file.
        """
        self.file = file
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

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        stream consumer with an open CSV writer.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer
        """
        f = self.file.writer()
        f.write(schema)
        return Write(writer=f)
