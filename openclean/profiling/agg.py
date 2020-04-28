# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Implementation of the main data profiling aggregation operators."""

from openclean.function.predicate.base import Tautology
from openclean.profiling.base import (
    DataStreamProfiler, DataStreamProfilerFactory
)


# -- Generic Statistic Computer -----------------------------------------------

class Aggregator(DataStreamProfiler):
    """Compute statistics over an aggregated stream of scalar (column) values.
    Evaluates a given function on the values in the stream first (optional).
    Maintains a list of resulting values. Applys a given statistical function
    (or list of functions) on the aggregated data to compute the end result.
    """
    def __init__(self, feature, stats, ignore_none=True):
        """Initialize the objct properties. The value feature function is
        applied to all values in the data stream. The results are maintained in
        an internal list. The statistics function(s) are then applied on the
        aggregated data to compute the final result. Input values for which the
        value function returns None are ignored (if the ignore None flag is
        True).

        Parameters
        ----------
        feature: callable
            Function that quantifies a feature for a single (scalar) value.
        stats: callable or list(callable)
            Callables that accept a list of values as their only argument.
        ignore_none: bool, optional
            Do not include None values (returned by the value function) in the
            internal value list if this flag is True.

        Raises
        ------
        ValueError
        """
        self.feature = feature
        self.stats = stats if isinstance(stats, list) else list([stats])
        self.ignore_none = ignore_none
        self.data = list()

    def compute(self):
        """Compute method of the scalar consumer. Returns the sum of values
        in the stream that were passed to the calculator.

        Returns
        -------
        list(int)
        """
        return [s(self.data) for s in self.stats]

    def consume(self, value):
        """Consume next value in the data stream. Adds the result of evaluating
        the value function on the given value to the maintained sum.

        Parameters
        ----------
        value: scalar
            Scalar value in the data stream.
        """
        val = self.feature(value)
        if val is not None or not self.ignore_none:
            self.data.append(val)


# -- Count --------------------------------------------------------------------

class Counter(DataStreamProfiler):
    """The counter evaluates a given predicate or condition on a data stream.
    It counts the number of times that the predicate evaluates to a given truth
    value.
    """
    def __init__(self, predicate=None, truth_value=True, count=0):
        """Initialize the object properties. If no predicate is given a
        tautology that always evaluates to the truth value is used. In this
        case, the counter returns the number of elements that it was passed.

        Parameters
        ----------
        predicate: callable, optional
            Callable that accepts a single scalar value as input and that
            returns a scalar value.
        truth_value: scalar, optional
            Return value of the predicate that is considered as True, i.e., the
            predicate is satisfied.
        count: int, optional
            Initialize the counter value.
        """
        if predicate is None:
            predicate = Tautology(truth_value)
        self.predicate = predicate
        self.truth_value = truth_value
        self.count = count

    def compute(self):
        """Compute method of the scalar consumer. Returns the number of values
        in the stream that satisfied the predicate, i.e., for which the result
        of the predicate equaled the truth value.

        Returns
        -------
        int
        """
        return list([self.count])

    def consume(self, value):
        """Consume next value in the data stream. Evaluates the predicate on
        the given value and checks if the result equals the truth value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.
        """
        if self.predicate(value) == self.truth_value:
            self.count += 1


class Count(DataStreamProfilerFactory):
    """Factory pattern for scalar stream value counter functions. The factory
    allows to instantiate a counter object for each column in a data frame
    separately.
    """
    def __init__(self, predicate, truth_value=True, count=0):
        """Initialize the properties that are passed on to the instantiated
        counter.

        Parameters
        ----------
        predicate: callable
            Callable that accepts a single scalar value as input and that
            returns a scalar value.
        truth_value: scalar, optional
            Return value of the predicate that is considered as True, i.e., the
            predicate is satisfied.
        count: int, optional
            Initialize the counter value.
        """
        self.predicate = predicate
        self.truth_value = truth_value
        self.count = count

    def get_profiler(self, df, column=None):
        """Create an instance of a counter object for values in a column of a
        given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        column: string-like object (e.g., openclean.data.column.Columns)
            Column in the data frame for which the counter is instantiated.

        Returns
        -------
        openclean.profiling.agg.Counter
        """
        return Counter(
            predicate=self.predicate,
            truth_value=self.truth_value,
            count=self.count
        )

    def result_size(self):
        """The returned counters will generate exactly one value per data
        stream.

        Returns
        -------
        int
        """
        return 1


# -- Helper functions ---------------------------------------------------------

def value(value):
    """Simple helper function for aggregators. Returns the value that it
    receives as an argument.

    Parameters
    ----------
    value: scalar
        Valie in a data stream.

    Returns
    -------
    scalar
    """
    return value
