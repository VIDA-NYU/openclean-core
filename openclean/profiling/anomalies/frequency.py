# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Operators for frequency outlier detection."""

from openclean.data.metadata import Feature
from openclean.data.stream import Stream
from openclean.function.value.normalize import divide_by_total
from openclean.profiling.anomalies.threshold import threshold_filter


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
    values = Stream(df=df, columns=columns)
    lookup = Feature(values).normalize(divide_by_total)
    return threshold_filter(values=lookup, threshold=threshold)
