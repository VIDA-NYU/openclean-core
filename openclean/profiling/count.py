# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for counters that count values in a data stream that satisfy a
given condition or predicate. In most cases, the data stream will be created
from the values in a data frame column. The counter factory pattern is required
for counters that are not universal but column specific, e.g., counting the
number of distinct values in a column.
"""

from openclean.function.predicate.scalar.null import IsEmpty, IsNotEmpty
from openclean.profiling.agg import Count
from openclean.profiling.base import (
    DataFrameStatsProfiler, DataStreamProfilerFactory
)
from openclean.profiling.distinct import DistinctValues


"""Row labels for the default counter predicates."""
DEFAULT_LABELS = ['is_empty', 'is_not_empty', 'distinct']


def counts(df, predicates=None, index=None):
    """The count operator evaluates a list of scalar predicates on each cell
    value for a input data frame. The resulting data frame has the same columns
    as the input data frame. Each row contains the counts (i.e., the number of
    times a predicate evaluated to True) for all input predicates and all
    columns. The order of rows in the resulting data frame corresponds to the
    order of predicates in the input list.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    predicates: list([
            callable ||
            openclean.profiling.base.DataStreamProfilerFactory
        ]), optional
        List of callable scalar predicates or profiler factories.
    index: list, optional
        Optional index for rows in the resulting data frame.

    Returns
    -------
    pandas.DataFrame
    """
    return Counts(predicates=predicates, index=index).transform(df)


class Counts(DataFrameStatsProfiler):
    """The count operator takes a list of scalar predicates as input. It
    evaluates the predicates for each row and each column in a given data
    frame. The result is a data frame where rows contain the counts for each
    predicate and each column.

    The operator expects either a callable as predicate or a profiler factory.
    The latter is useful to for counters that do not evaluate a predicate, for
    example a counter for distinct values in a column.
    """
    def __init__(self, predicates=None, index=None):
        """Initialize the list of scalar predicates. The optional index is used
        as index for the rows in the resulting data frame.

        If the list of predicates is no given a default set of predicates is
        used that counts the empty, non-empty, and distinct values for each
        columns. If the predicate list is not given, the index has to be either
        None as well or contain exactly three values.

        Parameters
        ----------
        predicates: list([
                callable ||
                openclean.profiling.base.DataStreamProfilerFactory
            ]), optional
            List of callable scalar predicates or profiler factories.
        index: list, optional
            Optional index for rows in the resulting data frame.

        Raises
        ------
        ValueError
        """
        if predicates is None:
            predicates = [IsEmpty(), IsNotEmpty(), DistinctValues()]
            if index is None:
                index = DEFAULT_LABELS
        # Convert callables to counter factories
        profilers = list()
        for pred in predicates:
            if not isinstance(pred, DataStreamProfilerFactory):
                profiler = Count(predicate=pred)
            else:
                profiler = pred
            profilers.append(profiler)
        super(Counts, self).__init__(profilers=profilers, index=index)
