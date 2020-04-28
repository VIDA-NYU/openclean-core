# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Generic conditional outlier detector. Identify values as outliers if they
satisfy a given outlier predicate.
"""

from openclean.profiling.anomalies.base import AnomalyDetector


class ConditionalOutliers(AnomalyDetector):
    """Detect outliers in a given value stream based on an outlier condition
    predicate. A value is considered an outlier if it satisfies the given
    predicate.
    """
    def __init__(self, predicate):
        """Initialize the object properties.

        Parameters
        ----------
        predicate: callable
            Function that accepts a single value (scalar or tuple) and returns
            a Boolean value. If the predicate is satisfied the value is
            considered an outlier value and added to the set of returned
            values.
        """
        self.predicate = predicate

    def predict(self, values):
        """Identify values in a given list of distinct values that satisfy the
        outlier condition.

        Parameters
        ----------
        values: iterable
            Iterable of scalar values or tuples of scalar values.

        Returns
        -------
        list
        """
        result = set()
        for value in values:
            if self.predicate(value):
                result.add(value)
        return list(result)
