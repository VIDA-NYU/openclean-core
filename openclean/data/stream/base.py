# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for data stream processing pipelines."""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from typing import Iterator, List, Tuple

from openclean.data.column import ColumnName


# _- Data frame readers and writers -------------------------------------------

class DatasetIterator(metaclass=ABCMeta):
    """Abstract class for iterators over rows in a data frame. Data frame
    iterators are also context managers and iterators. Therefore, in addition
    to the header method, implementations are expected to implement (i) the
    __enter__ and __exit__ methods for a context manager, and (ii) the __iter__
    and __next__ method for Python iterators.
    """
    pass


class DatasetStream(metaclass=ABCMeta):
    """Reader for data streams. Provides the functionality to open the stream
    for reading. Dataset reader should be able to read the same dataset
    multiple times.
    """
    def __init__(self, columns: List[ColumnName]):
        """Initialize the schema for the rows in this data stream iterator.

        Parameters
        ----------
        columns: list of string
            Schema for data stream rows.
        """
        self.columns = columns

    def iterrows(self) -> Iterator[Tuple[int, List]]:
        """Simulate the iterrows() function of a pandas DataFrame as it is used
        in openclean. Returns an iterator that yields pairs of row identifier
        and value list for each row in the streamed data frame.

        Returns
        -------
        iterator
        """
        with self.open() as f:
            for rowid, row in f:
                yield rowid, row

    @abstractmethod
    def open(self) -> DatasetIterator:
        """Open the data stream to get a iterator for the rows in the dataset.

        Returns
        -------
        openclean.data.stream.base.DatasetIterator
        """
        raise NotImplementedError()  # pragma: no cover
