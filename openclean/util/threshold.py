# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper functions that allow to specify different types of thresholds, e.g.,
lt, le, ge, gt. Instead of specifying thresholds as a single scalar value we
specify them as callables. This makes it easier to run the same function with
different thresholds like x > threshold, x >= threshold, etc..
"""

from typing import Callable, Union

import operator


# -- Generic threshold class --------------------------------------------------

class Threshold(object):
    """Generic thrshold class that evaluates a binary operator on a given value
    and a pre-defined threshold. The operator is called as op(value, threshold).
    """
    def __init__(self, op: Callable, threshold: float):
        """Initialize the operator and the threshold.

        Parameters
        ----------
        op: callable
            Binary operator that is evaluated as op(value, threshold).
        threshold: float
            Threshold value.
        """
        self.op = op
        self.threshold = threshold

    def __call__(self, value: Union[int, float]) -> bool:
        """Evaluate the threshold condition as op(value, threshold). Returns
        True if the threshold condition is satisfied.

        Parameters
        ----------
        value: int or float
            Value that is tested whether it satisfies the threshold condition.

        Returns
        -------
        bool
        """
        return self.op(value, self.threshold)


# -- Factories for different thresholds ---------------------------------------

def ge(threshold: float) -> Threshold:
    """Get an instance for a greater or equal than threshold.

    Parameters
    ----------
    threshold: float
        Threshold value.

    Returns
    -------
    openclean.util.threshold.Threshold
    """
    return Threshold(op=operator.ge, threshold=threshold)


def gt(threshold: float) -> Threshold:
    """Get an instance for a greater than threshold.

    Parameters
    ----------
    threshold: float
        Threshold value.

    Returns
    -------
    openclean.util.threshold.Threshold
    """
    return Threshold(op=operator.gt, threshold=threshold)


def le(threshold: float) -> Threshold:
    """Get an instance for a lower or equal than threshold.

    Parameters
    ----------
    threshold: float
        Threshold value.

    Returns
    -------
    openclean.util.threshold.Threshold
    """
    return Threshold(op=operator.le, threshold=threshold)


def lt(threshold: float) -> Threshold:
    """Get an instance for a lower than threshold.

    Parameters
    ----------
    threshold: float
        Threshold value.

    Returns
    -------
    openclean.util.threshold.Threshold
    """
    return Threshold(op=operator.lt, threshold=threshold)


def to_threshold(threshold: Union[Callable, int, float]) -> Callable:
    """Helper class to ensure that a given argument is a callable. If a scalar
    value is given the gt threshold will be returned by default if the value is
    lower than one. The ge threshold will be returned for scalar values that
    are one or greater.

    Parameters
    ----------
    threshold: callable, int or float
        Expects a callable or a numeric value that will be wrapped in a
        comparison operator.

    Retuns
    ------
    callable

    Raise
    -----
    ValueError
    """
    if not callable(threshold):
        if threshold >= 1:
            return ge(threshold)
        else:
            return gt(threshold)
    return threshold
