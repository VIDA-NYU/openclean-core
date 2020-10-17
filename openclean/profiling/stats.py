# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of statistics helper methods for profiling."""

from collections import Counter
from typing import Optional

import scipy.stats as sp


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
    pk = [float(count)/total for _, count in values.items()]
    return sp.entropy(pk=pk, base=2)
