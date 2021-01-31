# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Value function that randomly selects a value from a given list."""

from random import Random
from typing import List, Optional

from openclean.data.types import Value
from openclean.function.value.base import ConstantValue, UnpreparedFunction, ValueFunction


class RandomSelect(UnpreparedFunction):
    """Value function that implements a random selector. Selects a value from a
    given list of values during the preparation step. Returns a constant value
    function for the selected value as the preparation result.
    """
    def __init__(self, seed: Optional[int] = None, ignore_freq: Optional[bool] = False):
        """Initialize the optional seed for the random number generator. The
        ignore frequency flag determines whether the frequency of values in
        a given selection list is taken into consideration or ignored. In the
        latter case a value is randomly sampled from the distinct list of values
        instead of the full given value list.

        Parameters
        ----------
        seed: int, default=None
            Seed value for the random number generator (for reproducibility
            purposes).
        ignore_freq: bool, default=False
            If False a value will be selected randomly from a given list of
            values. If True, a value is selected from the list of distinct values.
        """
        self.rand = Random()
        if seed is not None:
            self.rand.seed(seed)
        self.ignore_freq = ignore_freq

    def prepare(self, values: List[Value]) -> ValueFunction:
        """Randomly select a value from the givne list. Returns a constant
        value function with the selected value as the result.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        openclean.function.value.base.ConstantValue
        """
        if len(values) == 1:
            # No need to select randomly if only one value is given.
            return ConstantValue(values[0])
        elif self.ignore_freq:
            return ConstantValue(self.rand.choice(list(set(values))))
        return ConstantValue(self.rand.choice(values))
