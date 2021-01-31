# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data frame transformation operator for sorting by data frame columns."""

from openclean.data.schema import as_list, select_clause
from openclean.operator.base import DataFrameTransformer


# -- Functions ----------------------------------------------------------------

def order_by(df, columns, reversed=None):
    """Sort operator for data frames. Sort columns are referenced by their
    name or index position.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string)
        Single column or list of column index positions or column names.
    reversed: list(bool), default=None
        Allows to specify for each sort column if sort order is reversed.
        If given, the length of this list has to match the length of the
        columns list.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return Sort(columns=columns, reversed=reversed).transform(df)


# -- Operators ----------------------------------------------------------------

class Sort(DataFrameTransformer):
    """Sort operator for data frames. Allows to sort a data frame by one or
    more columns. For each column, the sort order can be specified separately.
    """
    def __init__(self, columns, reversed=None):
        """Initialize the list of sort columns and their respective sort order.
        Raises a ValueError if the two lists are incompatible.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        reversed: list(bool), default=None
            Allows to specify for each sort column if sort order is reversed.
            If given, the length of this list has to match the length of the
            columns list.
        """
        self.columns = as_list(columns)
        # Ensure that th reversed list matches the number of elements in the
        # columns list.
        if reversed:
            if not isinstance(reversed, list):
                reversed = [reversed]
            if len(reversed) != len(self.columns):
                raise ValueError('incompatible lists for columns and reversed')
            self.reversed = reversed
        else:
            self.reversed = [False] * len(self.columns)

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
        # Get sort column names and their index positions for the sort columns.
        colnames, colidx = select_clause(df.columns, columns=self.columns)
        # If the list of column names contains duplicates we cannot sort the
        # data frame without temporarily renaming columns.
        if len(df.columns) != len(set(df.columns)):
            orig_columns = df.columns
            df.columns = ['C{}'.format(i) for i in range(len(orig_columns))]
            colnames = ['C{}'.format(i) for i in colidx]
        else:
            orig_columns = None
        # Convert reversed flags to ascending semantics expected by pandas
        # sort_values function.
        ascending = [not r for r in self.reversed]
        df = df.sort_values(by=colnames, ascending=ascending)
        # Revert column renaming.
        if orig_columns is not None:
            df.columns = orig_columns
        return df
