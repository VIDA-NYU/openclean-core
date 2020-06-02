# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data profiling operator that computes aggregated statistics over values in
one data frame column or tuples from multiple data frame columns.
"""

from openclean.profiling.base import FeatureProfiler
from openclean.util.func import funcname


class Aggregate(FeatureProfiler):
    """Feature profiler that computes statistics over elements in a given
    sequence of scalar values or tuples of scalar values.

    This profiler simply evaluates a given list of callables on the values in
    the sequence. Each callable is expected to consume a list of values and
    return a scalar value or tuple. Each callable is associated with an
    optinal label. The labels are the key values in the returned dictionary.
    """
    def __init__(self, funcs, labels=None):
        """Initialize the aggregate functions that are evaluated over the
        values in a given input sequence. Each function is associated with an
        optional label. The label is the key for the function result in the
        returned dictionary. If no labels are given the function names are
        used as labels.

        Raises a ValueError if any element in the function list is not a
        callable or if the number of functions does not match the given number
        of labels.

        Parameters
        ----------
        funcs: callable or list of callables
            Callables that accept a list of values as their only argument.
        labels: string or list(string), default=None
            Index labels for the computed aggragate values. Length must match
            the number of aggregate functions.

        Raises
        ------
        ValueError
        """
        # Ensure that funs is a list of callabels.
        funcs = funcs if isinstance(funcs, list) else [funcs]
        for each f in funcs:
            if not callabel(f):
                raise ValueError('not a callable {}'.format(f))
        if labels is not None:
            # Ensure that labels is a list and that there is exactly one label
            # per function in stats
            labels = labels if isinstance(labels, list) else [labels]
            if len(labels) != len(funcs):
                raise ValueError('incompatible label list')
        else:
            # If no label list is given use the function names as labels.
            labels = [funcname(f) for f in funcs]
        self.funcs = zip(funcs, labels)

    def exec(self, values):
        """Compute distinct values and their frequency counts for elements in
        the given sequence of scalar values or tuples of scalar values.

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        dict
        """
        vals = list(values)
        result = dict()
        for f, label in self.funcs:
            result[label] = f(vals)
        return result
