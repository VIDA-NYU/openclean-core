# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operators for frequency outlier detection."""

from openclean.data.sequence import Sequence
from openclean.function.value.comp import Eq, Gt
from openclean.function.value.normalize import DivideByTotal
from openclean.profiling.anomalies.conditional import ConditionalOutliers
from openclean.profiling.feature.distinct import distinct


def frequency_outliers(df, columns, threshold):
    """Detect frequency outliers for values (or value combinations) in one or
    more columns of a data frame. A value (combination) is considered an
    outlier if the relative frequency satisfies the given threshold predicate.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string)
        Single column or list of column index positions or column names.
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
    # Create the predicate as a lookup over the normalized frequencies of
    # values in the given columns.
    values = distinct(df=df, columns=columns)
    frequencies = values.normalize(DivideByTotal().prepare(values.values()))
    op = FrequencyOutliers(
        frequency=frequencies,
        threshold=threshold
    )
    return op.find(values=values)


class FrequencyOutliers(ConditionalOutliers):
    """Detect frequency outliers for values in a given list. A value is
    considered an outlier if its relative frequency in the list satisfies the
    given threshold predicate.
    """
    def __init__(self, frequency, threshold):
        """Initialize the frequency function for list values and the threshold.

        Parameters
        ----------
        frequency: callable
            Function that allows to lookup the relative frequency of a given
            value.
        threshold: callable
            Function that accepts a float (i.e., the relative frequency) and
            that returns a Boolean value. True indicates that the value
            (frequency) satisfies the value outlier condition.
        """
        # If the threshold is an integer or float create a greater than
        # threshold using the value (unless the value is 1 in which case we
        # use eq).
        self.threshold = get_threshold(threshold)
        self.frequency = frequency
        super(FrequencyOutliers, self).__init__(predicate=self.is_outlier)

    def is_outlier(self, value):
        """Test if the relative frequency of a given value satisfies the
        outlier predicate. Returns True if the value is considered a frequency
        outlier.

        Parameters
        ----------
        value: scalar or tuple
            Value that is being tested for the outlier condition.

        Returns
        -------
        bool
        """
        return self.threshold(self.frequency(value))


# -- Helper methods -----------------------------------------------------------

def get_threshold(threshold):
    """Ensure that the given threshold is a callable.

    Parameters
    ----------
    threshold: callable, int or float
        Expects a callable or a numeric value that will be wrapped in a
        comparison operator.

    Retuns
    ------
    callable

    Raise
    -----
    ValueError
    """
    # If the threshold is an integer or float create a greater than threshold
    # using the value (unless the value is 1 in which case we use eq).
    if type(threshold) in [int, float]:
        if threshold == 1:
            threshold = Eq(1)
        else:
            threshold = Gt(threshold)
    elif not callable(threshold):
        raise ValueError('invalid threshold constraint')
    return threshold
