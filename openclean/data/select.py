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

from typing import List, Tuple, Union

import pandas as pd

from openclean.data.types import Column, ColumnRef, Columns, Schema


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


def column_ref(schema: Schema, column: ColumnRef) -> Tuple[str, int]:
    """Get the column name and index position for a referenced column in the
    given schema. Columns may be referenced by their name or index. This
    function returns both, the name and the index of the referenced column.

    Raises a ValueError if an unknown column is referenced.

    Parameters
    ----------
    schema: list of string
        List of column names in a dataset schema
    column: int, string, or Column
        Reference to a column in the dataset schema.

    Returns
    -------
    tuple(string, int)
    """
    if isinstance(column, int):
        # Raise value error if the specified index is invalid
        try:
            colname = schema[column]
            colidx = column
        except IndexError as ex:
            raise ValueError(ex)
    elif isinstance(column, Column) and column.colidx is not None:
        colidx = column.colidx
        if colidx < 0 or colidx >= len(schema):
            msg = 'invalid column index <{} {} {} />'
            raise ValueError(msg.format(column, column.colid, column.colidx))
        if schema[colidx] != column:
            msg = 'column name mismatch  <{} {} {} />'
            raise ValueError(msg.format(column, column.colid, column.colidx))
        colname = column
    elif isinstance(column, str):
        colidx = -1
        for i in range(len(schema)):
            if schema[i] == column:
                colname = schema[i]
                colidx = i
                break
        # Raise value error if the column name is unknown
        if colidx == -1:
            raise ValueError('unknown column name {}'.format(column))
    else:
        raise ValueError("invalid column reference '{}'".format(column))
    return colname, colidx


def select_clause(
    schema: Schema,
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
    schema: List of string
        List of columns in a dataset schema.
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
    # Ensure that columns is a list.
    columns = as_list(columns)
    # Ensure that all elements in the selected column list are names.
    column_names = list()
    column_index = list()
    for col in columns:
        colname, colidx = column_ref(schema=schema, column=col)
        column_names.append(colname)
        column_index.append(colidx)
    return column_names, column_index
