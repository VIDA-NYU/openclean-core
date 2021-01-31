# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of statistics helper functions anc classes for profiling."""

from collections import Counter
from typing import Optional, Tuple

import scipy.stats as sp

from openclean.data.types import Scalar


# -- Helper Classes -----------------------------------------------------------

class MinMaxCollector(dict):
    """Consumer that identifies the minimum and maximum value over a stream of
    data. The class extends a dictionary for integration into profiling result
    dictionaries.
    """
    def __init__(
        self, first_value: Optional[Scalar] = None,
        minmax: Optional[Tuple[Scalar, Scalar]] = None
    ):
        """Initialize the minimum and maximum value. The class can either be
        initialized using a single value (first_value) or a pair of (min, max).

        If both parameters are given a ValueError is raised.

        Parameters
        ----------
        first_value: scalar, default=None
            First value in the data stream. This value is used as the initial
            minimum and maximum value.
        minmax: tuple, default=None
            Tuple of (minimum, maximum) value.
        """
        if first_value is not None and minmax is not None:
            raise ValueError('invalid arguments')
        elif minmax is not None:
            self['minimum'], self['maximum'] = minmax
        else:
            self['minimum'], self['maximum'] = first_value, first_value

    def consume(self, value: Scalar):
        """Consume a value in the data stream and adjust the minimum and
        maximum if necessary.

        Parameters
        ----------
        value: scalar
            Value in the data stream.
        """
        if value < self['minimum']:
            self['minimum'] = value
        elif value > self['maximum']:
            self['maximum'] = value

    @property
    def maximum(self):
        """Get the current maximum over all consumed values.

        Returns
        -------
        scalar
        """
        return self['maximum']

    @property
    def minimum(self):
        """Get the current minimum over all consumed values.

        Returns
        -------
        scalar
        """
        return self['minimum']


# -- Helper Functions ---------------------------------------------------------

def entropy(values: Counter, default: Optional[float] = None) -> float:
    """Compute the entropy for a given set of distinct values and their
    frequency counts.

    Returns the default value if the given counter is empty.

    Parameters
    ----------
    values: collections.Counter
        Counter with frequencies for a set of distinct values.

    Returns
    -------
    float
    """
    # Return the default value if the set of values is empty.
    if not values:
        return default
    # Compute entropy over frequencies for values in the given counter.
    total = float(sum(values.values()))
    pk = [float(count) / total for _, count in values.items()]
    return sp.entropy(pk=pk, base=2)
