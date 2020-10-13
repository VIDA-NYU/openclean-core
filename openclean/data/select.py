# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Columns in openclean data frames have a unique identifier and a column name.
The column class extends the Python String class to be able to be used as a
column value in a Pandas data frame.
"""

from typing import List, Optional, Tuple, Union

import pandas as pd

from openclean.data.column import Column, Columns


def as_list(columns: Columns) -> List[Union[int, str, Column]]:
    """Ensure that the given columns argument is a list.

    Parameters
    ----------
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.

    Returns
    -------
    list
    """
    if isinstance(columns, pd.Index):
        return list(columns)
    elif isinstance(columns, tuple):
        return list(columns)
    elif not isinstance(columns, list):
        return [columns]
    else:
        return columns


def select_clause(
    df: Union[pd.DataFrame, List[Union[str, Column]]],
    columns: Columns
) -> Tuple[List[str], List[int]]:
    """Get the list of column name objects and index positions in a data frame
    for list of columns that are specified either by name or by index position.

    The result is a tuple containing two lists: the list of column objects and
    the list of column index positions.

    Raises errors if invalid columns positions or unknown column names or types
    are provided.

    Parameters
    ----------
    df: pandas.DataFrame or list of column names.
        Pandas data frame or list of data frame columns.
    columns: int, string or list of int or string
        Single column reference or a list of column index positions or column
        names.

    Returns
    -------
    (list, list)

    Raises
    ------
    ValueError
    """
    # The first argument may either be a data frame (or object with a columns
    # property) or a list of column (names) from a data frame schema.
    try:
        schema = df.columns
    except AttributeError:
        schema = df
    # Ensure that columns is a list.
    columns = as_list(columns)
    # Ensure that all elements in the selected column list are names.
    column_names = list()
    column_index = list()
    for col in columns:
        if isinstance(col, int):
            # Raise value error if the specified index is invalid
            try:
                colname = schema[col]
                colidx = col
            except IndexError as ex:
                raise ValueError(ex)
        elif isinstance(col, Column) and col.colidx is not None:
            colidx = col.colidx
            if colidx < 0 or colidx >= len(schema):
                msg = 'invalid column index <{} {} {} />'
                raise ValueError(msg.format(col, col.colid, col.colidx))
            if schema[colidx] != col:
                msg = 'column name mismatch  <{} {} {} />'
                raise ValueError(msg.format(col, col.colid, col.colidx))
            colname = col
        elif isinstance(col, str):
            colidx = -1
            for i in range(len(schema)):
                if schema[i] == col:
                    colname = schema[i]
                    colidx = i
                    break
            # Raise value error if the column name is unknown
            if colidx == -1:
                raise ValueError('unknown column name {}'.format(col))
        else:
            raise ValueError("invalid column reference '{}'".format(col))
        column_names.append(colname)
        column_index.append(colidx)
    return column_names, column_index


def select_by_id(
    df: pd.DataFrame, colids: Union[int, List[int]],
    raise_error: Optional[bool] = True
) -> List[int]:
    """Get the list of column index positions in a data frame for list of
    columns that are referenced by unique column identifier.

    Raises errors if the identifier list contains values that do not reference
    columns in the data frame schema and the raise error flag is True.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas data frame.
    colids: int or list(int)
        Single column identifier or list of column identifier.
    raise_error: bool, default=True
        Raise error if the column list contains unknown identifier.

    Returns
    -------
    list

    Raises
    ------
    ValueError
    """
    # Use a lookup for identifier of columns that are renamed
    includecols = set(colids) if isinstance(colids, list) else set([colids])
    # Implemnted using the select operator. For this, we need to get the index
    # positions for thoses columns that are included in the result.
    select_clause = list()
    for colidx, col in enumerate(df.columns):
        try:
            # Attempt to access the column id. If the identifier is for a
            # column that is being rename add the index to the select clause.
            if col.colid in includecols:
                select_clause.append(colidx)
                includecols.remove(col.colid)
        except AttributeError:
            pass
    # Raise an error if not all of the identifier in includecols have been
    # encountered.
    if includecols and raise_error:
        missing = list(includecols)
        raise ValueError("unknown column identifier {}".format(missing))
    return select_clause
