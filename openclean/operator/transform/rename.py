# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions and classes that implement the column renaming operator in
openclean.
"""

from openclean.data.types import Column
from openclean.data.select import as_list, select_by_id
from openclean.operator.base import DataFrameTransformer


# -- Functions ----------------------------------------------------------------

def rename(df, columns, names):
    """The column rename operator returns a data frame where a given list of
    columns has been renamed. The renaming does not have to include all columns
    in the data frame. However, the given list of columns and new column names
    have to be of the same length.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string)
        Single column or list of column index positions or column names.
    names: string or list(string)
        Single name or list of names for the renamed columns.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return Rename(columns=columns, names=names).transform(df)


def rename_columns(df, colids, names):
    """Rename columns that are referenced by their unique identifier. Raises a
    ValueError if the column identifier list contains values that do not
    reference columns in the data frame schema.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    colids: int or list(int)
        Single column identifier or list of column indentifier.
    names: string or list(string)
        Single name or list of names for the renamed columns. The number of
        values in that list must match the number of column identifiers.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    # Get index positions for referenced columns.
    columns = select_by_id(df=df, colids=colids)
    return Rename(columns=columns, names=names).transform(df)


# -- Operators ----------------------------------------------------------------

class Rename(DataFrameTransformer):
    """Data frame transformer that renames a selected list of columns in a data
    frame. The output is a data frame that contains all rows and columns from
    an input data frame but with thoses columns that are listed in the given
    column list being renamed with the respective value in the given names
    list.
    """
    def __init__(self, columns, names):
        """Initialize the list of columns that are being renames and the list
        new column names. The length of both lists has to be equal. If scalar
        values are provided for either columns or names they are converted into
        lists.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        names: string or list(string)
            Single name or list of names for the renamed columns.

        Raises
        ------
        ValueError
        """
        self.columns = as_list(columns)
        self.names = names if isinstance(names, list) else [names]
        # Raise an error if the length of the list of columns and column names
        # do not match.
        if len(self.columns) != len(self.names):
            raise ValueError('incompatible list for columns and names')

    def transform(self, df):
        """Return a data frame that contains all rows and columns from an input
        data frame but with thoses columns that are listed in the given column
        list being renamed with the respective value in the given names list.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        # Start with a copy of columns in the data frame.
        renamed_columns = list(df.columns)
        # For each column in the columns list find the index position in the
        # data frame schema
        colidx = range(len(df.columns))
        for nidx in range(len(self.columns)):
            cidx = self.columns[nidx]
            if isinstance(cidx, str):
                # Find the first occurrence of a column with the given name.
                try:
                    cidx = next(i for i in colidx if df.columns[i] == cidx)
                except StopIteration:
                    raise ValueError('unknown column name {}'.format(cidx))
            # The variable cidx points to the column that is being renamed.
            try:
                col = df.columns[cidx]
            except IndexError as ex:
                raise ValueError(ex)
            if isinstance(col, Column):
                col = Column(colid=col.colid, name=self.names[nidx])
            else:
                col = self.names[nidx]
            renamed_columns[cidx] = col
        # Create a copy of the data frame and then replace columns with the
        # list of renamed columns
        result = df.copy()
        result.columns = renamed_columns
        return result
