# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper functions to transform data frames into lists or mappings and vice
versa.
"""

from typing import Dict, List, Tuple, Union

import pandas as pd

from openclean.data.schema import select_clause
from openclean.data.types import Columns, Scalar


def get_value(row: Union[List, Tuple], columns: List[int]) -> Union[Scalar, Tuple]:
    """Helper function to get the value for a single column or multiple columns
    from a data frame row. If columns contains only a single column index the
    value at that index position in the given row is returned. If columns
    contains multiple column indices a tuple with the row values for all the
    specified columns is returned.

    Parameters
    ----------
    row: list or tuple of scalar values
        Row in a data frame.
    columns: list of integer
        List of index positions for extracted column values.

    Returns
    -------
    scalar or tuple of scalar
    """
    if len(columns) == 1:
        return row[columns[0]]
    else:
        return tuple([row[c] for c in columns])


def repair_mapping(df: pd.DataFrame, key: Columns, value: Columns) -> Dict:
    """Create a lookup table from the given data frame that represents a repair
    mapping for a given combination of lookup key and target value. The key
    columns and value columns represet the columns from which the lookup key and
    mapping target value are generated.

    The resulting mapping is a dictionary that contains entries for all key
    values that were mapped to target values that are different from the key
    value.

    The function will raise an error if no unique mapping can be defined from
    the values in the given data frame.

    Parameters
    ----------
    df: pd.DataFrame
        Pandas data frame.
    key: Columns
        Single column or list of column names or index positions. The specified
        column(s) are used to generate the mapping key value.
    value: Columns
        Single column or list of column names or index positions. The specified
        column(s) are used to generate the mapping target value.

    Returns
    -------
    dict
    """
    # Get column indices for source and target values
    _, keyidx = select_clause(df.columns, key)
    _, valueidx = select_clause(df.columns, value)
    # Create the result mapping as a dictionary.
    mapping = dict()
    for row in df.itertuples(index=False, name=None):
        keyval = get_value(row, keyidx)
        targetval = get_value(row, valueidx)
        # Ignore row if key value and target value are the same.
        if keyval == targetval:
            continue
        # If the key value exists in the mapping and is mapped to a different
        # value than he target then an error is raised since the mapping is not
        # a function of the key values.
        if keyval in mapping:
            if mapping[keyval] != targetval:
                raise ValueError("different targets for '{}'".format(keyval))
        else:
            mapping[keyval] = targetval
    return mapping


def to_set(data):
    """Create a set of distinct values (rows) for a given data frame or data
    series. For data frames with multiple columns, each row is converted into
    a tuple that is added to the set.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.

    Returns
    -------
    sets

    Raises
    ------
    ValueError
    """
    if isinstance(data, pd.Series):
        return set(data)
    elif isinstance(data, pd.DataFrame):
        if len(data.columns) > 1:
            return set([t for t in data.itertuples(index=False, name=None)])
        else:
            return set(data.iloc[:, 0])
    raise ValueError('invalid type {}'.format(type(data)))
