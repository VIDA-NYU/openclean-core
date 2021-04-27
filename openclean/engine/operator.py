# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Stream operators are subclasses or wrappers for :class:`StreamProcessor`.
These operators can directly be applied on archive snapshots.
"""

from histore.document.operator import DatasetOperator
from typing import Optional

from openclean.data.stream.base import DataRow
from openclean.data.types import DatasetSchema
from openclean.operator.stream.consumer import StreamFunctionHandler
from openclean.operator.stream.processor import StreamProcessor


class StreamOperator(DatasetOperator):
    """Operator for processing rows in a data stream from a dataset archive
    snapshot.
    """
    def __init__(self, func: StreamFunctionHandler, description: Optional[str] = None):
        """Initialize the stream operator function for processing rows and the
        input descriptor.

        Parameters
        ----------
        func: openclean.operator.stream.consumer.StreamFunctionHandler
            Function for processing data rows. The function also contains the
            schema of the generated rows.
        description: string, default=None
            Optional user-provided description for the snapshot that is created
            by this operator.
        """
        super(StreamOperator, self).__init__(
            columns=func.columns,
            description=description,
        )
        self.func = func

    def handle(self, rowid: int, row: DataRow) -> DataRow:
        """Evaluate the operator on the given row.

        Returns the processed row. If the result is None this signals that the
        given row should not be part of the collected result.

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
        return self.func.handle(rowid=rowid, row=row)


class StreamOp(object):
    """Wrapper for a stream processor and an optioanl snapshot description."""
    def __init__(self, func: StreamProcessor, description: Optional[str] = None):
        """Initialize the stream processor and the input descriptor.

        Parameters
        ----------
        func: openclean.operator.stream.processor.StreamProcessor
            Processor for data rows.
        description: string, default=None
            Optional user-provided description for the snapshot that is created
            by this operator.
        """
        self.func = func
        self.description = description

    def open(self, schema: DatasetSchema) -> StreamOperator:
        """Factory pattern for stream consumer. Returns an instance of the
        stream consumer that corresponds to the action that is defined by the
        stream processor.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.engine.operator.DatasetOperator
        """
        return StreamOperator(
            func=self.func.open(schema=schema),
            description=self.description
        )
