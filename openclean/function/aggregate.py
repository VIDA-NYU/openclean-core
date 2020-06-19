# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of aggregation functions that return a computed statistic over a
list of values.
"""

from openclean.function.base import ValueFunction

import openclean.util as util


# -- Generic prepared statistics function -------------------------------------

class ColumnStats(ValueFunction):
    """Generic function for computing an aggregated statistic over a list of
    values. Provides the option to normalize the values in the list before the
    final result is computed.
    """
    def __init__(self, func, normalizer=None, value=None, name=None):
        """Initialize the statistics function and the optional normalizer.

        Parameters
        ----------
        func: callable
            Function that accepts values from one or more columns as input.
        normalizer: callable or openclean.function.base.ValueFunction,
                default=None
            Optional normalization function that will be used to normalize
            values before the aggregation function is executed.
        value: int or float, default=None
            Prepared result value for a list of values. If the value is None,
            the operator still needs to be prepared.
        name: string, default=None
            Unique function name for profiling functions.

        Raises
        ------
        ValueError
        """
        if not callable(func):
            raise ValueError('not a callable')
        if normalizer is not None:
            if not isinstance(normalizer, ValueFunction):
                raise ValueError('normalizer not a value function')
        # Ensure that funciton is callable and that the normalizer is a value
        # function (if given).
        super(ColumnStats, self).__init__(
            name=name if name else util.funcname(func)
        )
        self.func = func
        self.normalizer = normalizer
        self.value = value

    def eval(self, value):
        """The evaluation method returns the prepared value, i.e., the result
        that was computed by the prepare method for all input values.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        int or float
        """
        return self.value

    def exec(self, values):
        """Evaluate the aggregate function over values in a given sequence.
        Normalize the values if the associated normalizer is not None.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        scalar
        """
        if self.normalizer is not None:
            if not self.normalizer.is_prepared():
                f = self.normalizer.prepare(values)
            else:
                f = self.normalizer
            return self.func(f.apply(values))
        else:
            return self.func(values)

    def is_prepared(self):
        """The operator still needs to be prepared if the pre-computed value
        is None.

        Returns
        -------
        bool
        """
        return self.value is not None

    def prepare(self, values):
        """If the aggregator is used as a value function, the preparation step
        executes the associates statistics function over all values in the
        list and uses that value as the result for all calls to the eval
        method.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        openclean.function.aggregate.ColumnStats
        """
        return ColumnStats(
            func=self.func,
            normalizer=self.normalizer,
            value=self.exec(values),
            name=self.name()
        )


# -- Shortcuts for common statistics methods ----------------------------------

class Max(ColumnStats):
    """Aggregation function that returns the maximum from a list of values."""
    def __init__(self, normalizer=None, name=None):
        """Initialize the statistics function in the super class.

        Parameters
        ----------
        normalizer: callable or openclean.function.base.ValueFunction,
                default=None
            Optional normalization function that will be used to normalize
            values before the aggregation function is executed.
        name: string, default=None
            Unique function name for profiling functions.
        """
        super(Max, self).__init__(func=max, normalizer=normalizer, name=name)


class Mean(ColumnStats):
    """Aggregation function that returns the mean over a list of values."""
    def __init__(self, normalizer=None, name=None):
        """Initialize the statistics function in the super class.

        Parameters
        ----------
        normalizer: callable or openclean.function.base.ValueFunction,
                default=None
            Optional normalization function that will be used to normalize
            values before the aggregation function is executed.
        name: string, default=None
            Unique function name for profiling functions.
        """
        import numpy as np
        super(Mean, self).__init__(
            func=np.mean,
            normalizer=normalizer,
            name=name
        )


class Min(ColumnStats):
    """Aggregation function that returns the minimum from a list of values."""
    def __init__(self, normalizer=None, name=None):
        """Initialize the statistics function in the super class.

        Parameters
        ----------
        normalizer: callable or openclean.function.base.ValueFunction,
                default=None
            Optional normalization function that will be used to normalize
            values before the aggregation function is executed.
        name: string, default=None
            Unique function name for profiling functions.
        """
        super(Min, self).__init__(func=min, normalizer=normalizer, name=name)


class Sum(ColumnStats):
    """Aggregation function that returns the sum from a list of values."""
    def __init__(self, normalizer=None, name=None):
        """Initialize the statistics function in the super class.

        Parameters
        ----------
        normalizer: callable or openclean.function.base.ValueFunction,
                default=None
            Optional normalization function that will be used to normalize
            values before the aggregation function is executed.
        name: string, default=None
            Unique function name for profiling functions.
        """
        super(Sum, self).__init__(func=sum, normalizer=normalizer, name=name)
