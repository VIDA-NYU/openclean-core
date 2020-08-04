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

from openclean.function.value.base import CallableWrapper, ValueFunction
from openclean.profiling.base import profile, Profiler, ProfilingFunction

import openclean.util as util


# -- Predicate satisfaction counter -------------------------------------------

def count(df, columns=None, predicate=None, truth_value=True):
    """Count number of values in a given data frame that match a given value.

    Parameters
    ----------
    df: pandas.DataFramee
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    predicate: callable or openclean.function.base.ValueFunction
        Predicate that is evaluated over a list of values.
    truth_value: scalar, defaut=True
        Count the occurrence of the truth value when evaluating the column
        expression over the data frame.

    Returns
    -------
    int
    """
    # The distinct value generator will transform list values into tuples. We
    # need to do the same if the truth_value is a list.
    if isinstance(truth_value, list):
        truth_value = tuple(truth_value)
    counter = Count(predicate=predicate, truth_value=truth_value)
    return profile(df, columns=columns, profilers=counter)[counter.name]


class Count(ProfilingFunction):
    """Count number of values in a given list that satisfy a given predicate.
    """
    def __init__(self, predicate, truth_value=True, name=None):
        """Initialize the predicate that is being evaluated.

        Parameters
        ----------
        predicate: callable or openclean.function.base.ValueFunction
            Predicate that is evaluated over a list of values.
        truth_value: scalar, defaut=True
            Return value of the predicate that signals that the predicate is
            satisfied by an input value.
        name: string, default=None
            Count the occurrence of the truth value in a given set of values.
        """
        super(Count, self).__init__(
            name=name if name else util.funcname(predicate)
        )
        # Wrap the predicate if it is a simple callable.
        if predicate is not None:
            if not isinstance(predicate, ValueFunction):
                predicate = CallableWrapper(func=predicate)
        self.predicate = predicate
        self.truth_value = truth_value

    def run(self, values):
        """Count the number of values in the given sequence that satisfy the
        associated predicate.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        int
        """
        if self.predicate is not None:
            counter = 0
            for value, count in values.items():
                if self.predicate.eval(value) == self.truth_value:
                    counter += count
            return counter
        else:
            return values.get(self.truth_value, 0)


# -- Multi-predicate counts ---------------------------------------------------

class Counts(Profiler):
    """The count operator takes a list of scalar predicates as input. It
    evaluates the predicates for each value in a given sequence. the operator
    returns a dictionary that contains the results from the individual
    counters, keyed by their unique name.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the list of scalar predicates.

        Parameters
        ----------
        args: callable, openclean.function.base.ValueFunction, or
                openclean.profiling.count.Count
            Predicates that are evaluated over a list of values.
        truth_values: list, defaut=True
            Truth values (one per predicate) that are the result values of the
            respective predicate that indicate that the predicate is satisfied.
        name: string, default=None
            Optional unique function name.
        """
        # Ensure that truth values is a list.
        truth_values = kwargs.get('truth_values')
        if truth_values is not None:
            if not isinstance(truth_values, list):
                truth_values = [truth_values]
        else:
            truth_values = [True] * len(args)
        # Raise an error if the two lists are not of same length
        if len(args) != len(truth_values):
            raise ValueError('incompatible lists')
        counters = list()
        for f, truth_value in zip(args, truth_values):
            if not isinstance(f, Count):
                f = Count(predicate=f, truth_value=truth_value)
            counters.append(f)
        super(Counts, self).__init__(
            profilers=counters,
            name=kwargs.get('name', 'counts')
        )


# -- Value counts -------------------------------------------------------------

class Values(ProfilingFunction):
    """Count number of distinct and total values."""
    def __init__(
        self, distinct_count='distinct', total_count='total', name=None
    ):
        """Initialize the labels for the distinct and total count in the
        result.

        Parameters
        ----------
        distinct_count: string, defaut='distinct'
            Label for the distinct count value in the result.
        total_count: string, defaut='total'
            Label for the total count value in the result.
        name: string, default=None
            Count the occurrence of the truth value in a given set of values.
        """
        # Wrap the predicate if it is a simple callable.
        super(Values, self).__init__(name=name if name else 'valueCounts')
        self.distinct_count = distinct_count
        self.total_count = total_count

    def run(self, values):
        """Count the number of distinct and total values in the given set.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        dict
        """
        return {
            self.distinct_count: len(values),
            self.total_count: sum(values.values())
        }
