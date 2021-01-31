# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Funciton to compute set of distinct values and their frequencies from a list
of values.
"""

import pandas as pd

from collections import Counter
from typing import Callable, List, Optional, Tuple, Union

from openclean.data.types import Scalar
from openclean.function.eval.base import InputColumn, EvalFunction
from openclean.function.eval.base import evaluate, to_eval
from openclean.function.value.base import ValueFunction, normalize


def count(
    df: pd.DataFrame, predicate: Optional[EvalFunction] = None,
    truth_value: Optional[Scalar] = True
) -> int:
    """Count the number of rows in a data frame. If the optional predicate is
    given, the rows that satisy the predicate is counted.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    predicate: openclean.function.eval.base.EvalFunction, default=None
        Predicate that is evaluated over te rows in the data frame.
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
    for val in predicate.eval(df):
        if val == truth_value:
            count += 1
    return count


def distinct(
    df: pd.DataFrame,
    columns: Optional[Union[InputColumn, List[InputColumn]]] = None,
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
    columns: int, string, list, or openclean.function.eval.base.EvalFunction,
            default=None
        Evaluation function to extract values from data frame rows. This
        can also be a a single column reference or a list of column references.
        If not given the distinct number of rows is counted.
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
    # Evaluate the columns statement to get the values from which the set of
    # distinct values is generated.
    if columns is None:
        columns = list(df.columns)
    values = evaluate(df, to_eval(columns))
    # Evaluate the columns function on all rows in the data frame and count the
    # frequencies for the returned values.
    counts = Counter(values)
    # Normalize the result if a normalizer is given.
    if normalizer is not None:
        counts = normalize(
            values=counts,
            normalizer=normalizer,
            keep_original=keep_original,
            labels=labels
        )
    return counts
