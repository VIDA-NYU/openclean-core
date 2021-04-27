# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Datatype conversion consumer and processor for data pipelines."""

from typing import Optional

from openclean.data.types import DatasetSchema
from openclean.data.stream.base import DataRow
from openclean.operator.stream.consumer import ProducingConsumer, StreamConsumer
from openclean.operator.stream.processor import StreamProcessor
from openclean.profiling.datatype.convert import DatatypeConverter, DefaultConverter


class Typecast(ProducingConsumer, StreamProcessor):
    """Consumer for rows that casts all values in a row using a given type
    converter.
    """
    def __init__(
        self, converter: Optional[DatatypeConverter] = None,
        columns: Optional[DatasetSchema] = None,
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the datatype converter.

        Parameters
        ----------
        converter: openclean.profiling.datatype.convert.DatatypeConverter,
                default=None
            Datatype converter for values data stream. Uses the default
            converter if no converter is given.
        columns: list of string
            Names of columns for the rows that the consumer will receive.
        consumer: openclean.data.stream.base.StreamConsumer, default=None
            Downstream consumer for converted rows.
        """
        super(Typecast, self).__init__(columns=columns, consumer=consumer)
        if converter is None:
            converter = DefaultConverter()
        self.converter = converter

    def handle(self, rowid: int, row: DataRow) -> DataRow:
        """Convert all values in the given row to a datatype that is defined by
        the associated converter.

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
        return [self.converter.cast(value) for value in row]

    def open(self, schema: DatasetSchema) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        stream consumer that does the type casting for all data frame rows.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamConsumer
        """
        return Typecast(converter=self.converter, columns=schema)
