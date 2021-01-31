# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Value function that implements a conflict resolution stategy for majority
voting.
"""

from collections import Counter
from typing import List, Optional

from openclean.data.types import Value
from openclean.function.value.base import ConstantValue, UnpreparedFunction, ValueFunction


class MajorityVote(UnpreparedFunction):
    """Value function that implements a majority voting strategy for conflict
    resolution. Selects the most frequent value in a given list of values during
    the preparation step. Returns a constant value function for the selected
    value as the preparation result.

    If there is a tie for the most frequent value the tie is broken using a
    given tiebreaker function. If no tiebreaker function is specified an error
    will be raised.
    """
    def __init__(self, tiebreaker: Optional[ValueFunction] = None):
        """Initialize the tiebreaker function for situations where more than one
        value is the most frequent one.

        Parameters
        ----------
        tiebreaker: openclan.function.value.base.ValueFunction, default=None
            Value function that is called with the list of most frequent values
            in case that there is a tie.
        """
        self.tiebreaker = tiebreaker

    def prepare(self, values: List[Value]) -> ValueFunction:
        """Select the most frequent value in the given list of values. A constant
        value function is returned as the result.

        If there is a tie for the most frequent value the tie is broken using the
        tiebreaker function. If no tiebreaker function is specified a ValuError
        will be raised.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        openclean.function.value.base.ConstantValue
        """
        if len(values) == 1:
            # No need to count frequencies if only one value is given.
            return ConstantValue(values[0])
        # Get frequency counts for all values.
        counts = Counter(values)
        # The first value is the candidate for the voting outcome. We do need to
        # check, however, whether there are multiple values with equal frequency
        # than the most frequent value.
        mf_value, mf_count = counts.most_common(1)[0]
        if len(counts) > 1:
            # Check if the frequency of the second value equals that of the first
            # value (in which case we do have a tie).
            _, count = counts.most_common(2)[1]
            if count == mf_count:
                # Need to break the tie using the tiebreaker (if given). If no
                # tiebreaker was given a ValueError is raised.
                if self.tiebreaker is None:
                    raise ValueError('cannot break tie for majority vote')
                # Create a list of the most frequent values. This list repeats
                # each value as many times as its count.
                ties = Counter()
                for val, count in counts.most_common():
                    if count < mf_count:
                        # Done when we encounter the first value that has a
                        # frequency that is less than that of the most frequent
                        # values.
                        break
                    ties[val] = count
                most_frequent = ties.elements()
                # Prepare the tiebreaker function if necessary.
                f = self.tiebreaker if self.tiebreaker.is_prepared() else self.tiebreaker.prepare(most_frequent)
                mf_value = f.eval(most_frequent)
        return ConstantValue(mf_value)
