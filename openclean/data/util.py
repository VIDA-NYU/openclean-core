# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper functions to transform data frames into lists or mappings and vice
versa.
"""

import pandas as pd

from openclean.data.select import select_clause


def get_value(row, colidx):
    """Return the cell value(s) from a dat frame row for the columns with the
    given indices. If the column index list contains only a single element
    the result is the cell value of the respective column in the data frame
    row. If the column index list contains multiple values the result is a
    tuple containing the cell values from the respective columns.

    Parameters
    ----------
    row: pandas.core.series.Series
        Pandas data frame row object
    colidx: list(int)
        List of column indices.

    Returns:
    scalar or tuple
    """
    if len(colidx) == 1:
        return row[colidx[0]]
    else:
        return tuple([row[col] for col in colidx])


def to_lookup(df, key_columns=None, target_columns=None, override=True):
    """Create a lookup dictionary from a given data frame. Values in the key
    column(s) are mapped to corresponding values in the target column(s).

    If the override flag is True, duplicate keys in the key columns will
    override existing entries in the dictionary. Otherwise, a ValueError will
    be raised when a duplicate key occurs.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    key_columns: int or string or list(int or string), optional
        List of column index or column name for columns from which the mapping
        key is computed.
    target_columns: int or string or list(int or string), optional
        List of column index or column name for columns from which the mapping
        value combinations are computed.
    override: bool, optional
        Allow duplicate key values in the data frame key columns.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    lookup_dict = dict()
    # Get indices for key and target columns.
    if not isinstance(key_columns, list):
        key_columns = [key_columns]
    _, keycols = select_clause(df.columns, columns=key_columns)
    if not isinstance(target_columns, list):
        target_columns = [target_columns]
    _, targetcols = select_clause(df.columns, columns=target_columns)
    for _, values in df.iterrows():
        key = get_value(values, colidx=keycols)
        if not override and key in lookup_dict:
            raise ValueError('duplicate key {}'.format(key))
        lookup_dict[key] = get_value(values, colidx=targetcols)
    return lookup_dict


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
            result = set()
            for _, values in data.iterrows():
                result.add(tuple(values))
            return result
        else:
            return set(data.iloc[:, 0])
    elif isinstance(data, list):
        return set(data)
    elif isinstance(data, set):
        return data
    raise ValueError('invalid type {}'.format(type(data)))
