# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Consumer for data frame rows in a data processing pipeline. Stream consumer
will receive each row in a data stream for processing. The consumer is not
expected to provide the final result until the colse method is called.
"""

from abc import ABCMeta, abstractmethod
from typing import Any, List

from openclean.data.stream.base import DatasetStream


# -- Abstract base class for data stream consumers ----------------------------

class StreamConsumer(metaclass=ABCMeta):
    """Abstract class for consumers in a data stream processing pipeline. A
    consumer is the instantiation of a StreamProcessor that is prepared to
    process (consume) rows in a data stream.

    Each consumer may be is associated with an (optional) downstream consumer
    that will received the processed row from this operator. Consumers that
    are connected to a downstream consumer are also refered to as producers.
    Consumers that are not connected to a downstream consumer are called
    collectors. There are separate modules for each type of consumers.
    """
    @abstractmethod
    def close(self) -> Any:
        """Signal that the end of the data stream has reached. The return value
        is implementation dependent.

        Returns
        -------
        any
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def consume(self, rowid: int, row: List) -> List:
        """Consume the given row. Passes the processed row on to an associated
        downstream consumer. Returns the processed row. If the result is None
        this signals to a collector/iterator that the given row should not be
        part of the collected/yielded result.

        Parameters
        -----------
        rowid: int
            Unique row identifier
        row: list
            List of values in the row.
        """
        raise NotImplementedError()  # pragma: no cover

    def process(self, ds: DatasetStream) -> Any:
        """Consume a given data stream and return the computed result.

        Parameters
        ----------
        ds: from openclean.data.stream.base.DatasetStream
            Iterable stream of dataset rows.

        Returns
        -------
        any
        """
        for rid, row in ds.iterrows():
            self.consume(rowid=rid, row=row)
        return self.close()
