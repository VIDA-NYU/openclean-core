# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operators for frequency outlier detection."""

from openclean.function.value.normalize import DivideByTotal
from openclean.profiling.anomalies.base import AnomalyDetector
from openclean.profiling.base import profile
from openclean.profiling.util import get_threshold


ABSOLUTE = 'absolute'
NORMALIZED = 'normalized'


def frequency_outliers(df, columns, threshold, normalize=DivideByTotal()):
    """Detect frequency outliers for values (or value combinations) in one or
    more columns of a data frame. A value (combination) is considered an
    outlier if the relative frequency satisfies the given threshold predicate.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    threshold: callable
        Function that accepts a float (i.e., the relative frequency) and that
        returns a Boolean value. True indicates that the value (frequency)
        satisfies the value outlier condition.
    normalize: openclean.function.value.normalize.NormalizeFunction
        Function used to normalize frequency values befor evaluating the
        threshold constraint.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    # Create the predicate as a lookup over the normalized frequencies of
    # values in the given columns.
    op = FrequencyOutliers(threshold=threshold, normalize=normalize)
    return profile(df, columns=columns, profilers=op)[op.name]


class FrequencyOutliers(AnomalyDetector):
    """Detect frequency outliers for values in a given list. A value is
    considered an outlier if its relative frequency in the list satisfies the
    given threshold predicate.
    """
    def __init__(self, threshold, normalize=DivideByTotal()):
        """Initialize the frequency threshold.

        Parameters
        ----------
        threshold: callable
            Function that accepts a float (i.e., the relative frequency) and
            that returns a Boolean value. True indicates that the value
            (frequency) satisfies the value outlier condition.
        normalize: openclean.function.value.normalize.NormalizeFunction
            Function used to normalize frequency values befor evaluating the
            threshold constraint.
        """
        super(FrequencyOutliers, self).__init__(name='frequencyOutlier')
        self.threshold = get_threshold(threshold)
        self.normalize = normalize

    def run(self, values):
        """Normalize the frequency counts in the given mapping. Returns all
        values that satisfy the threshold constraint together with their
        normalized (and absolute) frequencies.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        dict
        """
        result = dict()
        if self.normalize is not None:
            f = self.normalize.prepare(values.values())
            for value, count in values.items():
                freq = f.eval(count)
                if self.threshold(freq):
                    result[value] = {'count': count, 'frequency': freq}
        else:
            for value, count in values.items():
                if self.threshold(count):
                    result[value] = count
        return result
