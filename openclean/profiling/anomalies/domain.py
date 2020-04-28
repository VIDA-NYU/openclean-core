# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Domain outlier detector."""

from openclean.data.stream import Stream
from openclean.function.value.domain import is_not_in
from openclean.profiling.anomalies.conditional import ConditionalOutliers


def domain_outlier(df, columns, domain, ignore_case=False):
    """The domain outlier detector returns the list of values from a given data
    stream that do not occur in a ground truth domain.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string)
        Single column or list of column index positions or column names.
    domain: pandas.DataFrame, pandas.Series, or object
        Data frame or series, or any object that implements the
        __contains__ method.
    ignore_case: bool, optional
        Ignore case for domain inclusion checking

    Returns
    -------
    list
    """
    predicate = is_not_in(domain=domain, ignore_case=ignore_case)
    op = ConditionalOutliers(predicate=predicate)
    return op.predict(values=set(Stream(df=df, columns=columns)))
