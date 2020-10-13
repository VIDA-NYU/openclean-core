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

from collections import Counter

from typing import Callable, Optional, Union

from openclean.data.types import Scalar
from openclean.function.value.base import CallableWrapper, ValueFunction
from openclean.profiling.base import Profiler, DistinctSetProfiler

import openclean.util as util


# -- Counters -----------------------------------------------------------------

class Count(DistinctSetProfiler):
    """Count number of values in a given list that satisfy a given predicate.
    """
    def __init__(
        self, predicate: Optional[Union[Callable, ValueFunction]] = None,
        truth_value: Optional[Scalar] = True, name: Optional[str] = None
    ):
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
        # Wrap the predicate if it is a simple callable.
        if predicate is not None:
            if not isinstance(predicate, ValueFunction):
                predicate = CallableWrapper(func=predicate)
        self.predicate = predicate
        self.truth_value = truth_value

    def process(self, values: Counter):
        """Count the number of values in the given sequence that satisfy the
        associated predicate.

        Parameters
        ----------
        values: collections.Counter
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


class Counts(Profiler):
    """The count operator takes a list of scalar predicates as input. It
    evaluates the predicates for each value in a given sequence. The operator
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
        counters = dict()
        for f, truth_value in zip(args, truth_values):
            key = util.funcname(f)
            if not isinstance(f, Count):
                f = Count(predicate=f, truth_value=truth_value)
            counters[key] = f
        super(Counts, self).__init__(profilers=counters)
