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

from openclean.data.sequence import Sequence
from openclean.function.base import (
    CallableWrapper, ProfilingFunction, ValueFunction
)
from openclean.profiling.base import Profiler


# -- Predicate satisfaction counter -------------------------------------------

def count(df, columns=None, predicate=None, truth_value=True):
    """Count number of values in a given list that satisfy an associated
    predicate.

    Parameters
    ----------
    df: pandas.DataFramee
        Input data frame.
    columns: int, string, or list(int or string), default=None
        Single column or list of column index positions or column names.
        Defines the list of value (pairs) for which the predicate is evaluated.
    predicate: callable or penclean.function.base.ValueFunction
        Predicate that is evaluated over a list of values.
    truth_value: scalar, defaut=True
        Return value of the predicate that signals that the predicate is
        satisfied by an input value.
    """
    counter = Count(predicate=predicate, truth_value=truth_value)
    return counter.exec(values=Sequence(df=df, columns=columns))


class Count(ProfilingFunction):
    """Count number of values in a given list that satisfy an associated
    predicate.
    """
    def __init__(self, predicate, truth_value=True, name=None):
        """Initialize the predicate and the unique function name. The predicate
        is expected to be a ValueFunction or a callable.

        Parameters
        ----------
        predicate: callable or openclean.function.base.ValueFunction
            Predicate that is evaluated over a list of values.
        truth_value: scalar, defaut=True
            Return value of the predicate that signals that the predicate is
            satisfied by an input value.
        name: string, default=None
            Optional unique function name. If no name is given, the name of the
            predicate function is used as the default.
        """
        if not isinstance(predicate, ValueFunction):
            predicate = CallableWrapper(predicate)
        super(Count, self).__init__(
            name=name if name else predicate.name()
        )
        self.predicate = predicate
        self.truth_value = truth_value

    def exec(self, values):
        """Count the number of values in the given sequence that satisfy the
        associated predicate.

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        int
        """
        if not self.predicate.is_prepared():
            f = self.predicate.prepare(values)
        else:
            f = self.predicate
        count = 0
        for val in values:
            if f.eval(val) == self.truth_value:
                count += 1
        return count


# -- Multi-predicate counts ---------------------------------------------------

def counts(df, columns=None, predicates=None):
    """The count operator evaluates a list of scalar predicates on a list of
    values from data frame column(s). It returns a dictionary that contains
    the results from the individual counters, keyed by their unique name.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string), default=None
        Single column or list of column index positions or column names.
        Defines the list of value (pairs) for which the predicates are
        evaluated.
    predicates: list of callable, openclean.function.base.ValueFunction, or
            openclean.profiling.count.Count
        Predicates that are evaluated over a list of values.

    Returns
    -------
    dict
    """
    values = Sequence(df=df, columns=columns)
    return Counts(predicates=predicates).exec(values=values)


class Counts(Profiler):
    """The count operator takes a list of scalar predicates as input. It
    evaluates the predicates for each value in a given sequence. the operator
    returns a dictionary that contains the results from the individual
    counters, keyed by their unique name.
    """
    def __init__(self, predicates, name=None):
        """Initialize the list of scalar predicates.

        Parameters
        ----------
        predicates: list of callable, openclean.function.base.ValueFunction, or
                openclean.profiling.count.Count
            Predicates that are evaluated over a list of values.
        name: string, default=None
            Optional unique function name.
        """
        if not isinstance(predicates, list):
            predicates = [predicates]
        counters = list()
        for f in predicates:
            if not isinstance(f, Count):
                f = Count(predicate=f)
            counters.append(f)
        super(Counts, self).__init__(
            profilers=counters,
            name=name if name else 'counts'
        )
