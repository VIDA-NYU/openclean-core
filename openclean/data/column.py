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

import pandas as pd


class Column(str):
    """Columns in openclean data frames are subclasses of Python strings that
    contain a unique column identifier. This implementation is based on:
    https://bytes.com/topic/python/answers/32098-my-experiences-subclassing-string

    The order of creation is that the __new__ method is called which returns
    the object then __init__ is called.
    """
    def __new__(cls, colid, name, *args, **keywargs):
        """Initialize the String object with the given column name. Ignore the
        column identifier.

        Parameters
        ----------
        colid: int
            Unique column identifier
        name: string
            Column name
        """
        return str.__new__(cls, str(name))

    def __init__(self, colid, name):
        """Initialize the unique column identifier. The column name has already
        been initialized by the __new__ method that is called prior to the
        __init__ method.

        Parameters
        ----------
        colid: int
            Unique column identifier
        name: string
            Column name
        """
        self.colid = colid


# -- Helper functions ---------------------------------------------------------

def as_list(columns):
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
    elif not isinstance(columns, list):
        return [columns]
    else:
        return columns


def select_clause(df, columns):
    """Get the list of column name objects and index positions in a data frame
    for list of columns that are specified either by name or by index position.

    The result is a tuple containing two lists: the list of column objects and
    the list of column index positions.

    Raises errors if invalid columns positions or unknown column names are
    provided.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas data frame.
    columns: list(int or str)
        List of column index positions or column names.

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
        if isinstance(col, int):
            # Raise value error if the specified index is invalid
            try:
                colname = df.columns[col]
                colidx = col
            except IndexError as ex:
                raise ValueError(ex)
        else:
            colidx = -1
            for i in range(len(df.columns)):
                try:
                    if df.columns[i] == col:
                        colname = df.columns[i]
                        colidx = i
                        break
                except ValueError:
                    print('DF {}'.format(df.columns[i]))
                    print('COL {}'.format(col))
            # Raise value error if the column name is unknown
            if colidx == -1:
                raise ValueError('unknown column name {}'.format(col))
        column_names.append(colname)
        column_index.append(colidx)
    return column_names, column_index
