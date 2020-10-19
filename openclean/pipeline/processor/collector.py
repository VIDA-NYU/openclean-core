# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Stream processors that are collectors are stream operators that represent
stream consumers that are collectors in the data stream.
"""

from __future__ import annotations
from typing import List, Optional

from openclean.data.stream.base import DatasetStream
from openclean.pipeline.consumer.base import StreamConsumer
from openclean.pipeline.consumer.collector import DataFrame, Write
from openclean.pipeline.processor.base import StreamProcessor


class CollectOperator(StreamProcessor):
    """Stream operator that represents the endpoint in a data stream. This is
    a generic operator for any consumer that does not require access to the
    data stream columns when it is instantiated.
    """
    def __init__(self, cls: StreamConsumer, *args, **kwargs):
        """Initialize the class of the stream consumer that is instantiated
        when the open method is called. The class constructor will receive any
        additional arguments that are passed to this constructor.

        Parameters
        ----------
        cls: class definition of openclean.data.stream.consumer.StreamConsumer
            Class of the consumer that is instantiated by the operator.
        args: variable argument list
            Additional arguments for the consumer class constructor.
        kwrgs: variable keyword arguments
            Additional keywrod arguments for the consumer class constructor.
        """
        self.cls = cls
        self.args = args
        self.kwargs = kwargs

    def open(
        self, ds: DatasetStream,
        schema: List[str],
        upstream: Optional[List[StreamProcessor]] = None,
        downstream: Optional[List[StreamProcessor]] = None
    ) -> StreamConsumer:
        """Create an instance of the associated consumer class as the sink in a
        data stream processing pipeline. Will ignore any given downstream
        operators.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input.
        schema: list of string
            List of column names in the data stream schema.
        upstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of upstream operators for the received data stream. Ignored.
        downstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of downstream operators for the generated consumer. Ignored,

        Returns
        -------
        openclean.data.stream.consumer.StreamConsumer
        """
        return self.cls(*self.args, **self.kwargs)


class DataFrameOperator(StreamProcessor):
    """Stream operator that returns a data frame collector as the generated
    consumer. Ignores any given downstram operators.
    """
    def open(
        self, ds: DatasetStream,
        schema: List[str],
        upstream: Optional[List[StreamProcessor]] = None,
        downstream: Optional[List[StreamProcessor]] = None
    ) -> DataFrame:
        """Create a data frame collector as the sink in a data stream
        processing pipeline. Will ignore any given downstream operators.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input.
        schema: list of string
            List of column names in the data stream schema.
        upstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of upstream operators for the received data stream. Ignored.
        downstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of downstream operators for the generated consumer. Ignored,

        Returns
        -------
        openclean.data.stream.consumer.DataFrame
        """
        return DataFrame(columns=schema)


class WriteOperator(StreamProcessor):
    """Operator that writes the rows in a data stream to a file on the local
    file system.
    """
    def __init__(self, file):
        """Initialize the reference to the output file.

        Parameters
        ----------
        file: openclean.data.stream.csv.CSVFile
            Output CSV file for all rows in the data stream that are passed to
            a consumer for this operator.
        """
        self.file = file

    def open(
        self, ds: DatasetStream,
        schema: List[str],
        upstream: Optional[List[StreamProcessor]] = None,
        downstream: Optional[List[StreamProcessor]] = None
    ) -> Write:
        """Create a consumer that writes all rows that it receives to the
        associated CSV file.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input.
        schema: list of string
            List of column names in the data stream schema.
        upstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of upstream operators for the received data stream. Ignored.
        downstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of downstream operators for the generated consumer. Ignored,

        Returns
        -------
        openclean.data.stream.consumer.Write
        """
        return Write(self.file.write(header=schema))
