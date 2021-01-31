# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions that filter values in a given list."""

from openclean.function.value.base import CallableWrapper, ValueFunction


def filter(values, predicate, truth_value=True):
    """Filter values in a list based on a given predicate function. Returns
    a new list containing only those values that satisfy the given predicate.

    Parameters
    ----------
    values: list
        List of scalar values or tuples of scalar values.
    predicate: openclean.function.value.base.ValueFunction or callable
        Callable or value function that is used to filter values in a given
        list.
    truth_value: scalar, defaut=True
        Return value of the predicate that signals that the predicate is
        satisfied by an input value.
    """
    return Filter(predicate, truth_value).apply(values)


class Filter(object):
    """Filter values in a list based on a given predicate function."""
    def __init__(self, predicate, truth_value=True):
        """Initialize the predicate that is used to filter values in a list.

        Parameters
        ----------
        predicate: openclean.function.value.base.ValueFunction or callable
            Callable or value function that is used to filter values in a given
            list.
        truth_value: scalar, defaut=True
            Return value of the predicate that signals that the predicate is
            satisfied by an input value.
        """
        if not isinstance(predicate, ValueFunction):
            predicate = CallableWrapper(func=predicate)
        self.predicate = predicate
        self.truth_value = truth_value

    def apply(self, values):
        """FIlter values in the given list using the associated predicate. Only
        values for which the predicate is satisfied will be returned in the
        resulting list.

        Calls the prepare method of an associated value function before
        executing the eval method on each individual value in the given list.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        list
        """
        result = list()
        f = self.predicate.prepare(values)
        for v in values:
            if f.eval(v) == self.truth_value:
                result.append(v)
        return result
