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
from openclean.data.load import stream
from openclean.data.select import as_list
from openclean.function.value.base import ValueFunction, normalize


def count(df: pd.DataFrame) -> int:
    """Count the number of rows in a data frame.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.

    Returns
    -------
    int
    """
    return df.shape[0]


def distinct(
    df: pd.DataFrame, columns: Optional[Columns] = None,
    normalizer: Optional[Union[Callable, ValueFunction]] = None,
    keep_original: Optional[bool] = False,
    labels: Optional[Union[List[str], Tuple[str, str]]] = None
) -> Counter:
    """Compute the set of distinct value combinations for a given list of
    columns. Returns a dictionary containing the distinct values (tuples)
    together with their frequency counts.

    If the optional normalization function is given, the frequency counts in
    the returned dictionary will be normalized. If the keep original flag is
    True, the returned dictionary will map key values to nested dictionaries
    that contain the original and the normalized value.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
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
    # Ensure that columns are a list if given.
    if columns is not None:
        columns = as_list(columns)
        counts = stream(df).distinct(*columns)
    else:
        counts = stream(df).distinct()
    # Normalize the result if a normalizer is given.
    if normalizer is not None:
        counts = normalize(
            values=counts,
            normalizer=normalizer,
            keep_original=keep_original,
            labels=labels
        )
    return counts
