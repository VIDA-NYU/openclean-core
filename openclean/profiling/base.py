# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base class for operators that perform data profiling on a sequence
of data values.

Profilers can perform a wide range of tasks on a given sequence of values. Some
profiling operators compute one or more features for all values in the sequence
(e.g., frequency). Other examples of profilers detect outliers in a sequence of
values. That is, they filter values based on some condition computed over the
value features. Profilers can also compute new 'value', for example, when
discovering patterns in the data.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from collections import Counter
from typing import Dict, List, Union

import pandas as pd

from openclean.data.types import Scalar
from openclean.function.eval.base import InputColumn, evaluate, to_eval


"""Type alias for data profiler results. Data profilers return either a
dictionary or a list of values. The structure of either is implementation
dependent.
"""
ProfilerResult = Union[Dict, List]


# --Profler base classes ------------------------------------------------------

class DataProfiler(metaclass=ABCMeta):
    """Profiler for a stream of (scalar) values. A data profiler computes
    statistics (informative summaries) over all values in a data stream, i.e.,
    values from a single column or multiple columns in a dataset.

    Data profiler are stream-aware so that an implementation of a profiler can
    be used on data frames as well as with streams over rows in a dataset.

    Data is passed to the profiler either as pairs of (value, count) where
    count is a frequency count (using the methods open, consume, close) or as a
    Counter with distinct values and their absolute counts (using the process
    method). In the case of a stream of (value, count)-pairs, the values in the
    stream are not guaranteed to be unique, i.e., the same value may be passed
    to the profiler multiple times (with potentially different counts).

    The profiler returns a dictionary or a list with the profiling results. The
    elements and structure of the result are implementation dependent.
    """
    @abstractmethod
    def close(self) -> ProfilerResult:
        """Signal the end of the data stream. Returns the profiling result. The
        type of the result is a dictionary. The elements and structure in the
        dictionary are implementation dependent.

        Returns
        -------
        dict or list
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def consume(self, value: Scalar, count: int):
        """Consume a pair of (value, count) in the data stream. Values in the
        stream are not guaranteed to be unique and may be passed to this
        consumer multiple times (with multiple counts).

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
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def open(self):
        """Singnal the start of the data stream. This method can be used by
        implementations of the scalar profiler to initialize internal
        variables.
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def process(self, values: Counter) -> ProfilerResult:
        """Compute one or more features over a set of distinct values. This is
        the main profiling function that computes statistics or informative
        summaries over the given data values. It operates on a compact form of
        a value list that only contains the distinct values and their frequency
        counts.

        The return type of this function is a dictionary. The elements and
        structure in the dictionary are implementation dependent.

        Parameters
        ----------
        values: collections.Counter
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        dict or list
        """
        raise NotImplementedError()  # pragma: no cover

    def run(
        self, df: pd.DataFrame, columns: Union[InputColumn, List[InputColumn]]
    ) -> ProfilerResult:
        """Run the profiler using values that are generated from one or more
        columns (producers) for a given data frame. Evaluates the producers
        and creates a value count that is passed on to the process method for
        profiling.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.
        columns: int, string, list, or
                openclean.function.eval.base.EvalFunction
            Evaluation function to extract values from data frame rows. This
            can also be a a single column reference or a list of column
            references.

        Returns
        -------
        dict or list
        """
        producers = to_eval(producers=columns)
        return self.process(Counter(evaluate(df=df, producers=producers)))


class DataStreamProfiler(DataProfiler):
    """Data stream profiler that implements the process method of the profiler
    function using the stream methods consume and close.
    """
    def process(self, values: Counter) -> ProfilerResult:
        """Compute one or more features over a set of distinct values. Streams
        the elements in the given counter to the consume method.

        Parameters
        ----------
        values: collections.Counter
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        dict or list
        """
        self.open()
        for value, count in values.items():
            self.consume(value=value, count=count)
        return self.close()


class DistinctSetProfiler(DataProfiler):
    """Profiling function that collects all elements in the stream and then
    uses the process method to compute the profiling result.
    """
    def __init__(self):
        """Initialize the counter object for frequencies of distinct values in
        a data stream.
        """
        self.values = None

    def close(self) -> ProfilerResult:
        """Signal the end of the data stream. Returns the profiling result. The
        type of the result is a dictionary. The elements and structure in the
        dictionary are implementation dependent.

        Returns
        -------
        dict or list
        """
        return self.process(self.values)

    def consume(self, value: Scalar, count: int):
        """Consume a pair of (value, count) in the data stream. Collects all
        values in a counter dictionary.

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
        self.values[value] += count

    def open(self):
        """Initialize the counter at the beginning of the stream."""
        self.values = Counter()
