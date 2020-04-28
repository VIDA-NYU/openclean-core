# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operator for threshold based outlier detection."""

from openclean.function.value.comp import get_threshold
from openclean.profiling.anomalies.conditional import ConditionalOutliers


def threshold_filter(values, threshold):
    """Detect values in a given list that satisfy the threshold constraint. The
    list of values is expected to be a single feature vector where values are
    mapped to numeric values on which the threshold constraint is evaluated.
    Returns the list of values for which the associated value satisfies the
    threshold constraint.

    Parameters
    ----------
    values: dict or openclean.data.metadata.Feature
        List of values mapped to a numeric feature.
    threshold: callable
        Function that accepts a float (i.e., the relative frequency) and that
        returns a Boolean value. True indicates that the value (frequency)
        satisfies the value outlier condition.

    Returns
    -------
    list

    Raises
    ------
    ValueError
    """
    # If the threshold is an integer or float create a greater than threshold
    # using the value (unless the value is 1 in which case we use eq).
    threshold = get_threshold(threshold)

    def predicate(value):
        """Evaluate the threshold predicate on the pre-computed frequency for
        the given value.
        """
        return threshold(values.get(value))

    op = ConditionalOutliers(predicate=predicate)
    return op.predict(values=values.keys())
