# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data frame transformation operator for sorting by data frame columns."""

from openclean.data.select import as_list, select_clause, select_by_id
from openclean.operator.base import DataFrameTransformer


# -- Functions ----------------------------------------------------------------

def movecols(df, columns, pos):
    """Move one or more columns in a data frame to a given position.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string)
        Single column or list of column index positions or column names.
    pos: int
        Insert position for the moved columns.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return MoveColumns(columns=columns, pos=pos).transform(df)


def move_columns(df, colids, pos):
    """Move one or more columns in a data frame to a given position. Columns
    are referenced by their unique column identifier.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    colids: int or list(int)
        Single column identifier or list of column indentifier.
    pos: int
        Insert position for the moved columns.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    columns = select_by_id(df=df, colids=colids)
    return MoveColumns(columns=columns, pos=pos).transform(df)


def move_rows(df, rowids, pos):
    """Move one or more rows in a data frame to a given position.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    rows: int or list(int)
        Identifier of rows that are being moved.
    pos: int
        Insert position for the moved columns.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return MoveRows(rows=rowids, pos=pos).transform(df)


# -- Operators ----------------------------------------------------------------

class MoveColumns(DataFrameTransformer):
    """Operator to move one or more columns to a specified index position."""
    def __init__(self, columns, pos):
        """Initialize the list of columns that are being moved and their new
        index position.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        pos: int
            Insert position for the moved columns.
        """
        self.columns = as_list(columns)
        self.pos = pos

    def transform(self, df):
        """Return a data frame that contains all rows but only those columns
        from the given input data frame that are included in the select clause.

        Raises a value error if the list of columns contains an item that
        cannot be matched to a column in the given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        # Ensure that column target position is valid.
        if self.pos is not None:
            if self.pos < 0 or self.pos > len(df.columns):
                raise ValueError('invalid target position {}'.format(self.pos))
            inspos = self.pos
        else:
            inspos = len(df.columns)
        # Get sort column names and their index positions for the sort columns.
        _, colidx = select_clause(df, columns=self.columns)
        colorder = [i for i in range(len(df.columns)) if i not in colidx]
        if inspos < len(colorder):
            colorder = colorder[:inspos] + colidx + colorder[inspos:]
        else:
            colorder = colorder + colidx
        # If the list of column names contains duplicates we cannot reorder the
        # data frame columns without temporarily renaming columns.
        if len(df.columns) != len(set(df.columns)):
            orig_columns = df.columns
            df.columns = ['C{}'.format(i) for i in range(len(orig_columns))]
        else:
            orig_columns = None
        # Reorder columns in the data frame.
        sortcols = [df.columns[i] for i in colorder]
        df = df[sortcols]
        # Revert column renaming.
        if orig_columns is not None:
            df.columns = [orig_columns[i] for i in colorder]
        return df


class MoveRows(DataFrameTransformer):
    """Operator to move one or more rows to a specified index position."""
    def __init__(self, rows, pos):
        """Initialize the list of rows that are being moved and their new
        index position.

        Parameters
        ----------
        rows: int or list(int)
            Identifier of rows that are being moved.
        pos: int
            Insert position for the moved columns.
        """
        self.rows = as_list(rows)
        self.pos = pos

    def transform(self, df):
        """Return a data frame that contains all rows but only those columns
        from the given input data frame that are included in the select clause.

        Raises a value error if the list of columns contains an item that
        cannot be matched to a column in the given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        # Ensure that column target position is valid.
        if self.pos is not None:
            if self.pos < 0 or self.pos > len(df.index):
                raise ValueError('invalid target position {}'.format(self.pos))
            inspos = self.pos
        else:
            inspos = len(df.index)
        # Reorder rows based on the modified order.
        roworder = [rid for rid in df.index if rid not in self.rows]
        if inspos < len(roworder):
            roworder = roworder[:inspos] + self.rows + roworder[inspos:]
        else:
            roworder = roworder + self.rows
        return df.reindex(roworder)
