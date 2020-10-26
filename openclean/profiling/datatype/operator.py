# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Datatype conversion consumer and processor for data pipelines."""

from typing import List, Optional, Tuple

from openclean.data.types import Schema
from openclean.data.stream.base import DatasetStream
from openclean.data.stream.base import StreamConsumer
from openclean.pipeline.consumer.producer import ProducingConsumer
from openclean.pipeline.processor.producer import ProducingOperator
from openclean.profiling.datatype.convert import (
    DatatypeConverter, DefaultConverter
)


class Typecast(ProducingConsumer):
    """Consumer for rows that casts all values in a row using a given type
    converter.
    """
    def __init__(
        self, converter: Optional[DatatypeConverter] = None,
        consumer: Optional[StreamConsumer] = None
    ):
        """Initialize the datatype converter.

        Parameters
        ----------
        converter: openclean.profiling.datatype.convert.DatatypeConverter,
                default=None
            Datatype converter for values data stream. Uses the default
            converter if no converter is given.
        consumer: openclean.data.stream.base.StreamConsumer, default=None
            Downstream consumer for converted rows.
        """
        super(Typecast, self).__init__(consumer)
        if converter is None:
            converter = DefaultConverter()
        self.converter = converter

    def handle(self, rowid: int, row: List) -> List:
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


class TypecastOperator(ProducingOperator):
    """Definition of a typecast operator for definitions of a stream processing
    pipeline. This operator yields a producing consumer that converts the raw
    type of all cell values for rows in a data stream.
    """
    def __init__(self, converter: Optional[DatatypeConverter] = None):
        """Initialize the data value converter.

        Parameters
        ----------
        converter: openclean.profiling.datatype..convert.DatatypeConverter,
                default=None
            Datatype converter for values data stream. Uses the default
            converter if no converter is given.
        """
        self.converter = converter

    def create_consumer(
        self, ds: DatasetStream
    ) -> Tuple[Typecast, Schema]:
        """Create a typecast consumer that will convert the cell values for all
        rows in a data stream.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input. The data
            stream can be used to prepare any associated evaluation functions
            that are used by the returned consumer.

        Returns
        -------
        openclean.profiling.datatype.Typecast
        """
        return Typecast(converter=self.converter), ds.columns
