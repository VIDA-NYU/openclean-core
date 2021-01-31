# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper class for testing profiling functionality."""

from collections import Counter

from openclean.data.types import Scalar
from openclean.profiling.base import DataStreamProfiler


class ValueCounter(DataStreamProfiler):
    """Test profiler that collects the values and counts that are passed to it
    in a Counter.
    """
    def __init__(self):
        """Create the internal counter variable."""
        self.counter = None

    def close(self) -> Counter:
        """Return the counter at the end of the stream.

        Returns
        -------
        collections.Counter
        """
        return self.counter

    def consume(self, value: Scalar, count: int):
        """Add value and count to the internal counter.

        Parameters
        ----------
        value: scalar
            Scalar column value from a dataset that is part of the data stream
            that is being profiled.
        count: int
            Frequency of the value. Note that this count only relates to the
            given value and does not necessarily represent the total number of
            occurrences of the value in the stream.
        """
        self.counter[value] += count

    def open(self):
        """Initialize an empty counter at the beginning of the stream."""
        self.counter = Counter()
