# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Stream operator for creating a dataset archive snapshot from the output of
a stream pipeline.
"""

from histore.document.row import DocumentRow
from histore.document.stream import DocumentConsumer
from histore.key.base import NumberKey
from typing import List

from openclean.data.stream.base import DataRow
from openclean.data.types import Column, Schema
from openclean.operator.stream.consumer import StreamConsumer
from openclean.operator.stream.processor import StreamProcessor


class ArchiveLoader(StreamProcessor):
    """Processor that writes a stream of dataset rows to a dataset archive."""
    def __init__(self, schema: List[Column], version: int, consumer: DocumentConsumer):
        """Initialize the expected archive columns, the snapshot version and the
        consumer for document rows.

        ----------
        schema: list of openclean.data.types.Column
            List of columns (with unique identifier). The order of entries in
            this list corresponds to the order of columns in the stream schema.
        version: int
            Unique identifier for the new snapshot version.
        consumer: histore.document.stream.DocumentConsumer
            Consumer for rows in the stream.
        """
        self.schema = schema
        self.version = version
        self.consumer = consumer

    def open(self, schema: Schema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        archive writer.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer
        """
        return ArchiveWriter(
            schema=self.schema,
            version=self.version,
            consumer=self.consumer
        )


class ArchiveWriter(StreamConsumer):
    """Consumer that transforms stream rows into document rows and passes them
    to an associated archive writer (document consumer).
    """
    def __init__(self, schema: List[Column], version: int, consumer: DocumentConsumer):
        """Initialize the columns for the dataset snapshot (i.e., the snapshot
        schema), the snapshot version and the consumer for document rows.

        ----------
        schema: list of openclean.data.types.Column
            List of columns (with unique identifier). The order of entries in
            this list corresponds to the order of columns in the stream schema.
        version: int
            Unique identifier for the new snapshot version.
        consumer: histore.document.stream.DocumentConsumer
            Consumer for rows in the stream.
        """
        self.schema = schema
        self.version = version
        self.consumer = consumer
        # Initialize counter for row positions.
        self._pos = 0

    def close(self):
        """Close has not effect."""
        pass

    def consume(self, rowid: int, row: DataRow):
        """Create document row for the given stream row and pass it to the
        associated document consumer.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        values = {c.colid: val for c, val in zip(self.schema, row)}
        self.consumer.consume_document_row(
            row=DocumentRow(key=NumberKey(rowid), pos=self._pos, values=values),
            version=self.version
        )
        self._pos += 1
