# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Value function that selects a value from a given list based on a given
aggregator.
"""

from collections import defaultdict, Counter
from typing import Callable, List, Optional, Union

from openclean.data.types import Value
from openclean.function.value.text import to_len
from openclean.function.value.base import ConstantValue, UnpreparedFunction, ValueFunction


class ValueAggregator(UnpreparedFunction):
    """Value function that can be used to select a value from a list based on a
    given aggregation function. Passes a list of values to an aggregator and
    returns a constant value function with teh aggregator result. Allows to
    apply a feature generator on the given values prior to applying the aggregator.
    If a feature function is given the value that is associated with the selected
    feature is returned (and not the feature value itself).
    """
    def __init__(
        self, aggr: Callable, feature: Optional[Union[Callable, ValueFunction]] = None,
        tiebreaker: Optional[ValueFunction] = None
    ):
        """Initialize the aggregation function and the optional feature generator.
        If a feature generator is used there is a chance that more than one value
        is selected by the aggregator. In this case the resulting tie has to be
        resulved using a tiebreaker function.


        Parameters
        ----------
        aggr: callable
            Callable aggregator function that returns a single value for a
            given list of values.
        feature: callable or openclean.function.value.base.ValueFunction
            Function that is applied on values to extract a feature for each value.
            The aggregator is then applied on the extracted features and the
            original values that is associated with the selected feature is returned.
        tiebreaker: openclan.function.value.base.ValueFunction, default=None
            Value function that is called with the list of most selected values
            in case that there is a tie.
        """
        self.aggr = aggr
        self.feature = feature
        self.tiebreaker = tiebreaker

    def prepare(self, values: List[Value]) -> ValueFunction:
        """Evaluate the aggregation function on the given list of values. Returns
        a constant function for the selected value.

        If the feature generator is set, we first generate a feature for each
        value in the given list. We maintain the set of original values with
        each feature value. The aggregator is applied on the list of generated
        feature values and a constant functions for the original value that is
        associated with the selected feature is returned.

        If there is more than one value associated with a selected feature, the
        tiebreaker function is evaluated on all associated values. If not
        tiebraker was specified a ValueError is raised.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        openclean.function.value.base.ConstantValue
        """
        if self.feature is None:
            # Evaluate the aggregator directly on the list of values if no
            # feature function was specified.
            return ConstantValue(self.aggr(values))
        # Create a mapping of feature values to the original values.
        features = defaultdict(Counter)
        for val in values:
            features[self.feature(val)][val] += 1
        candidates = features[self.aggr(list(features.keys()))]
        if len(candidates) == 1:
            return ConstantValue(list(candidates.keys())[0])
        # Need to break the tie using the tiebreaker (if given). If no tiebreaker
        # was given a ValueError is raised.
        if self.tiebreaker is None:
            raise ValueError('cannot break tie for multiple selected candidates')
        candidates = list(candidates)
        f = self.tiebreaker if self.tiebreaker.is_prepared() else self.tiebreaker.prepare(candidates)
        return ConstantValue(f.eval(candidates))


class Longest(ValueAggregator):
    """Aggregator that selects the longest value from a given list of values."""
    def __init__(self, tiebreaker: Optional[ValueFunction] = None):
        """Initialize the aggregator, feature function and the tiebreaker function
        for cases where there are multiple values of the same length.

        Parameters
        ----------
        tiebreaker: openclan.function.value.base.ValueFunction, default=None
            Value function that is called with the list of most longest values
            in case that there is a tie.
        """
        super(Longest, self).__init__(aggr=max, feature=to_len, tiebreaker=tiebreaker)


class Max(ValueAggregator):
    """Aggregator that selects the maximum value from a given list of values."""
    def __init__(self):
        """Initialize the aggregator in the super class."""
        super(Max, self).__init__(aggr=max)


class Min(ValueAggregator):
    """Aggregator that selects the minimum value from a given list of values."""
    def __init__(self):
        """Initialize the aggregator in the super class."""
        super(Min, self).__init__(aggr=min)


class Shortest(ValueAggregator):
    """Aggregator that selects the shortest value from a given list of values."""
    def __init__(self, tiebreaker: Optional[ValueFunction] = None):
        """Initialize the aggregator, feature function and the tiebreaker function
        for cases where there are multiple values of the same length.

        Parameters
        ----------
        tiebreaker: openclan.function.value.base.ValueFunction, default=None
            Value function that is called with the list of most shortest values
            in case that there is a tie.
        """
        super(Shortest, self).__init__(aggr=min, feature=to_len, tiebreaker=tiebreaker)
