# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Feature function that computes normalized frequencies for values in a list.
"""

from openclean.data.metadata import Feature
from openclean.function.value.base import ValueFunction
from openclean.function.value.normalize import MinMaxScale


class NormalizedFrequency(ValueFunction):
    """Value function that computes a normalized frequency for values in a
    given list of values.
    """
    def __init__(self):
        """Initialize the object variables. All variables are initialiy set to
        None. The values will be initialized by the prepare method.
        """
        self.normalizer = None
        self.mapping = None

    def eval(self, value):
        """Return the normalized frequency for the given value.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        float
        """
        return self.normalizer.eval(self.mapping.get(value))

    def prepare(self, values):
        """Compute the frequency for each value to be used as the feature
        function. Then initialize the normalization function using the list
        of value frequencies.

        Parameters
        ----------
        values: list
            List of value sin the stream.

        Returns
        -------
        callable
        """
        self.mapping = Feature(values)
        self.normalizer = MinMaxScale().prepare(self.mapping.values())
        return self
