# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data profiling operator that computes statistics over aggregated values in
data frame columns.
"""

from openclean.profiling.agg import Aggregator
from openclean.profiling.base import (
    DataFrameStatsProfiler, DataStreamProfilerFactory
)

import openclean.util as util


# -- Factory for Statistics Aggregators ---------------------------------------

class Stats(DataStreamProfilerFactory):
    """Factory pattern for stream consumer that compute statistics over
    aggregated values in a data stream.
    """
    def __init__(self, feature, stats, index=None, ignore_none=True):
        """Initialize the objct properties. The value feature function is
        applied to all values in the data stream. The statistics function(s)
        are applied on the aggregated data for each stream to compute the final
        result. Input values for which the value function returns None are
        ignored (if the ignore None flag is True).

        Parameters
        ----------
        feature: callable
            Function that quantifies a feature for a single (scalar) value.
        stats: callable or list(callable)
            Callables that accept a list of values as their only argument.
        index: string or list(string),
            Index labels for the computed aggragate values. Length must match
            the number of stats functions.
        ignore_none: bool, optional
            Do not include None values (returned by the value function) in the
            internal value list if this flag is True.

        Raises
        ------
        ValueError
        """
        self.feature = feature
        self.stats = stats if isinstance(stats, list) else list([stats])
        if index is not None:
            # Ensure that the index is a list and that there is exactly one
            # entry per function in stats
            index = index if isinstance(index, list) else list([index])
            if len(index) != len(self.stats):
                raise ValueError('incompatible index list')
        else:
            # If not index list is given use the function names for the feature
            # and the statistical methods to create a row index.
            index = list()
            fname = util.funcname(self.feature)
            for s in self.stats:
                index.append('{}_{}'.format(fname, util.funcname(s)))
        self.index = index
        self.ignore_none = ignore_none

    def get_profiler(self, df, column=None):
        """Create an instance of an aggregator for values in a column of a
        given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        column: string-like object, optional
            Column in the data frame for which the counter is instantiated.

        Returns
        -------
        openclean.profiling.agg.Aggregator
        """
        return Aggregator(
            feature=self.feature,
            stats=self.stats,
            ignore_none=self.ignore_none
        )

    def result_size(self):
        """Each stats function will create one value in the computed result of
        each consumer.

        Returns
        -------
        int
        """
        return len(self.stats)


# -- Operators ----------------------------------------------------------------

def colstats(df, fstats):
    """Compute statistics over all columns in a data frame. Each object in the
    fstats list is a  function (factory) to compute a set of statistics (e.g.,
    min, max, mean) for a feature of the values in the data frame column.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    fstats: list(openclean.profiling.stats.Stats)
        List of factories for data stream statistics computers.

    Returns
    -------
    pandas.DataFrame
    """
    return ColumnStats(fstats=fstats).transform(df)


class ColumnStats(DataFrameStatsProfiler):
    """The count operator takes a list of scalar predicates as input. It
    evaluates the predicates for each row and each column in a given data
    frame. The result is a data frame where rows contain the counts for each
    predicate and each column.

    The operator expects either a callable as predicate or a profiler factory.
    The latter is useful to for counters that do not evaluate a predicate, for
    example a counter for distinct values in a column.
    """
    def __init__(self, fstats):
        """Initialize the list of scalar predicates. The optional index is used
        as index for the rows in the resulting data frame.

        If the list of predicates is no given a default set of predicates is
        used that counts the empty, non-empty, and distinct values for each
        columns. If the predicate list is not given, the index has to be either
        None as well or contain exactly three values.

        Parameters
        ----------
        fstats: list(openclean.profiling.stats.Stats)
            List of factories for data stream statistics computers.
        """
        self.fstats = fstats if isinstance(fstats, list) else list([fstats])
        # The row index for the resulting data frame is the concatenation of
        # index labels for the statistics that are computed by the individual
        # stats computers.
        index = list()
        for s in self.fstats:
            index = index + s.index
        super(ColumnStats, self).__init__(
            profilers=fstats,
            index=index,
            dtype=float
        )
