# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Feature function that computes normalized frequencies for values in a list.
"""

from collections import Counter

from openclean.function.value.base import ValueFunction
from openclean.function.value.normalize import MinMaxScale


class NormalizedFrequency(ValueFunction):
    """Value function that computes a normalized frequency for values in a
    given list of values.
    """
    def __init__(self, normalizer=None, mapping=None):
        """Initialize the object variables. All variables are initialiy set to
        None. The values will be initialized by the prepare method.

        Parameters
        ----------
        normalizer: openclean.function.value.normalize.MinMaxScale,
                default=None
            Normalization function.
        mapping: dict, default=None
            Lookup table for value frequencies.
        """
        self.normalizer = normalizer
        self.mapping = mapping

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

    def is_prepared(self):
        """The object still requires preparation if either of the internal
        variables is still None.

        Returns
        -------
        bool
        """
        return self.normalizer is not None and self.mapping is not None

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
        openclean.embedding.feature.frequency.NormalizedFrequency
        """
        mapping = Counter(values)
        normalizer = MinMaxScale().prepare(mapping.values())
        return NormalizedFrequency(mapping=mapping, normalizer=normalizer)
