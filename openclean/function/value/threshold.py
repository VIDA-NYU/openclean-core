# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Boolean functions that represent threshold predicates. Threshold predicates
compare a given value against a threshold value using a comparison operator (on
of <, <=, >, >=).
"""

from typing import Callable

import operator

from openclean.data.types import Value
from openclean.function.value.base import PreparedFunction


class ThresholdPredicate(PreparedFunction):
    """Base class for threshold constraints. This is a wrapper that maintains
    the threshold value and the comparison operator.
    """
    def __init__(self, threshold: float, op: Callable):
        """Initialize the threshold value and the comparison operator.

        Parameters
        ----------
        threshold: float
            Threshold value
        op: callable
            Binary comparison operator that compares a given value against the
            defined threshold.
        """
        self.threshold = threshold
        self.op = op

    def eval(self, value: Value) -> bool:
        """Evaluate the threshold predicate on a given value.

        Parameters
        ----------
        value: scalar or tuple
            Value that is comared against the treshold.

        Returns
        -------
        bool
        """
        return self.op(value, self.threshold)


class LowerOrEqual(ThresholdPredicate):
    """Lower than or equal threshold predicate (value <= threshold)."""
    def __init__(self, threshold: float):
        """Initialize the threshold value.

        Parameters
        ----------
        threshold: float
            Threshold value
        """
        super(LowerOrEqual, self).__init__(threshold=threshold, op=operator.le)


class LowerThan(ThresholdPredicate):
    """Lower than threshold predicate (value < threshold)."""
    def __init__(self, threshold: float):
        """Initialize the threshold value.

        Parameters
        ----------
        threshold: float
            Threshold value
        """
        super(LowerThan, self).__init__(threshold=threshold, op=operator.lt)


class GreaterOrEqual(ThresholdPredicate):
    """Greater than or equal threshold predicate (value >= threshold)."""
    def __init__(self, threshold: float):
        """Initialize the threshold value.

        Parameters
        ----------
        threshold: float
            Threshold value
        """
        super(GreaterOrEqual, self).__init__(threshold=threshold, op=operator.ge)


class GreaterThan(ThresholdPredicate):
    """Lower than threshold predicate (value > threshold)."""
    def __init__(self, threshold: float):
        """Initialize the threshold value.

        Parameters
        ----------
        threshold: float
            Threshold value
        """
        super(GreaterThan, self).__init__(threshold=threshold, op=operator.gt)
