# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operators in a data stream pipeline represent the actors in the definition
of the pipeline. Each operator provides the functionality (factory pattern) to
instantiate the operator for a given data stream before a data streaming
pipeline is executed.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import List, Optional

from openclean.data.stream.base import DatasetStream
from openclean.data.stream.base import StreamConsumer


# -- Stream operators ---------------------------------------------------------

class StreamProcessor(metaclass=ABCMeta):
    """Stream processors represent definitions of actors in a data stream
    processing pipeline. Each operator implements the open method to create
    a prepared instance (consumer) for the operator that is used in a stream
    processing pipeline to filter, manipulate or profile data stream rows.
    """
    @abstractmethod
    def open(
        self, ds: DatasetStream,
        schema: List[str],
        upstream: Optional[List[StreamProcessor]] = None,
        downstream: Optional[List[StreamProcessor]] = None
    ) -> StreamConsumer:
        """Factory pattern for stream consumer. Returns an instance of the
        stream consumer that corresponds to the action that is defined by the
        stream processor.

        Parameters
        ----------
        ds: openclean.data.stream.base.DatasetStream
            Data stream that the consumer will receive as an input. Use this
            stream in combination with any upstream operators to prepare any
            associated evaluation functions that are used by the returned
            consumer.
        schema: list of string
            List of column names in the data stream schema.
        upstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of upstream operators for the received data stream.
        downstream: list of openclean.pipeline.processor.base.StreamProcessor,
                default=None
            List of downstream operators for the generated consumer.

        Returns
        -------
        openclean.data.stream.base.StreamConsumer
        """
        raise NotImplementedError()  # pragma: no cover
