# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Funciton to compute set of distinct values and their frequencies from a list
of values.
"""

import pandas as pd

from collections import Counter
from typing import Callable, List, Optional, Tuple, Union

from openclean.data.column import Columns
from openclean.data.select import as_list
from openclean.data.types import Scalar
from openclean.function.eval.base import EvalFunction, Cols, Col
from openclean.function.value.base import ValueFunction, normalize


"""Type alias for specifying distinct columns or column combinations. Allows to
have an evaluation functions as the data source.
"""
DistinctColumns = Union[Columns, EvalFunction]


def count(
    df: pd.DataFrame, predicate: Optional[EvalFunction] = None,
    truth_value: Optional[Scalar] = True
) -> int:
    """Count the number of rows in a data frame. If the predicate is given then
    we only count the number of rows that satisy the predicate.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    predicate: openclean.function.eval.base.EvalFunction, default=None
        Predicate that is evaluated over a list of values.
    truth_value: scalar, defaut=True
        Count the occurrence of the truth value when evaluating the predicate
        on a the data frame rows.

    Returns
    -------
    int
    """
    # Return the number of rows in the data frame if no predicate is given.
    # Here we ignore the truth_value.
    if predicate is None:
        return df.shape[0]
    # Stream the data frame and filter rows using the given predicate. Returns
    # the count for rows that satisfy the predicate.
    count = 0
    f = predicate.prepare(df)
    for rowid, values in df.iterrows():
        if f.eval(values) == truth_value:
            count += 1
    return count


def distinct(
    df: pd.DataFrame, columns: Optional[DistinctColumns] = None,
    normalizer: Optional[Union[Callable, ValueFunction]] = None,
    keep_original: Optional[bool] = False,
    labels: Optional[Union[List[str], Tuple[str, str]]] = None
) -> Counter:
    """Compute the set of distinct value combinations for a single columns, a
    given list of columns, or the list of values returned by a given evaluation
    function. Returns a Counter containing the distinct values (tuples in case
    of multiple input columns) together with their frequency counts.

    If the optional normalization function is given, the frequency counts in
    the returned dictionary will be normalized. If the keep original flag is
    True, the returned dictionary will map key values to nested dictionaries
    that contain the original and the normalized value.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, list, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a a single column reference or a list of column references.
    normalizer: callable or openclean.function.value.base.ValueFunction,
            default=None
        Optional normalization function that will be used to normalize the
        frequency counts in the returned dictionary.
    keep_original: bool, default=False
        If the keep original value is set to True, the resulting dictionary
        will map key values to dictionaries. Each nested dictionary will
        have two elements, the original ('absolute') value and the
        normalized value.
    labels: list or tuple, default=('absolute', 'normalized')
        List or tuple with exactly two elements. The labels will only be
        used if the keep_original flag is True. The first element is the
        label for the original value in the returned nested dictionary and
        the second element is the label for the normalized value.

    Returns
    -------
    dict
    """
    # Create an evaluation function to extract values if the columns argument
    # is not an evaluation function.
    if columns is None or not isinstance(columns, EvalFunction):
        if columns is None:
            columns = Cols(*list(range(len(df.columns))))
        else:
            columns = as_list(columns)
            if len(columns) == 1:
                columns = Col(columns[0])
            else:
                columns = Cols(*columns)
    # Evaluate the columns function on all rows in the data frame and count the
    # frequencies for the returned values.
    counts = Counter()
    f = columns.prepare(df)
    for _, row in df.iterrows():
        counts[f.eval(list(row))] += 1
    # Normalize the result if a normalizer is given.
    if normalizer is not None:
        counts = normalize(
            values=counts,
            normalizer=normalizer,
            keep_original=keep_original,
            labels=labels
        )
    return counts
