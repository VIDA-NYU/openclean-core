# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data frame transformation operator that inserts new columns and rows into a
data frame.
"""

import pandas as pd

from openclean.function.base import EvalFunction, FullRowEval
from openclean.function.constant import Const
from openclean.operator.base import DataFrameTransformer


# -- Functions ----------------------------------------------------------------

def insert(df, names, values, pos=None):
    """Synonym for inscol function.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    names: string, or list(string)
        Names of the inserted columns.
    values: scalar, list, callable, or openclean.function.base.EvalFunction
        Single value, list of constant values, callable that accepts a data
        frame row as the only argument and returns a (list of) value(s)
        matching the number of columns inserted or an evaluation function
        that returns a matchin number of values.
    pos: int, optional
        Insert position for the new columns. If None the columns will be
        appended.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return inscol(df=df, names=names, values=values, pos=pos)


def inscol(df, names, pos=None, values=None):
    """Insert function for data frames columns. Returns a modified data frame
    where columns have been inserted at a given position. Exactly one column is
    inserted for each given column name. If the insert position is undefined,
    columns are appended to the data frame. If the position does not reference
    a valid position (i.e., not between 0 and len(df.columns)) a ValueError is
    raised.

    Values for the inserted columns are generated using the given callable.
    The funciton is expected to return exactly one value for each of the
    inserted columns.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    names: string, or list(string)
        Names of the inserted columns.
    values: scalar, list, callable, or openclean.function.base.EvalFunction
        Single value, list of constant values, callable that accepts a data
        frame row as the only argument and returns a (list of) value(s)
        matching the number of columns inserted or an evaluation function
        that returns a matchin number of values.
    pos: int, optional
        Insert position for the new columns. If None the columns will be
        appended.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return InsCol(names=names, pos=pos, values=values).transform(df)


def insrow(df, pos=None, values=None):
    """Insert a row into a data frame at a specified position. If the list of
    row values is given there has to be exactly one value per column in the
    data frame.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    pos: int, optional
        Insert position for the new row(s). If None the rows will be appended.
    values: list, optional
        List or values (inserts one row) or list of lists (inserts multiple
        rows).

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return InsRow(pos=pos, values=values).transform(df)


# -- Operators ----------------------------------------------------------------

class InsCol(DataFrameTransformer):
    """Data frame transformer that inserts columns into a data frame. Values
    for the new column(s) are generated using a given value generator function.
    """
    def __init__(self, names, pos=None, values=None):
        """Initialize the list of column names, the insert position and the
        function that is used to generate values for the inserted column(s).

        Parameters
        ----------
        names: string, or list(string)
            Names of the inserted columns.
        pos: int, optional
            Insert position for the new columns. If None the columns will be
            appended.
        values: scalar,
                list,
                callable, or
                openclean.function.base.EvalFunction, optional
            Single value, list of constant values, callable that accepts a data
            frame row as the only argument and returns a (list of) value(s)
            matching the number of columns inserted or an evaluation function
            that returns a matchin number of values.

        Raises
        ------
        ValueError
        """
        # Ensure that names is a list
        self.names = names if isinstance(names, list) else [names]
        self.pos = pos
        if values is not None:
            # If values is not a callable initialize a constant value function.
            if not callable(values):
                if isinstance(values, list):
                    values = Const(values)
                elif len(self.names) > 1:
                    values = Const([values] * len(names))
                else:
                    values = Const(values)
            elif not isinstance(values, EvalFunction):
                # Wrap the callable in a full row evaluation function
                values = FullRowEval(values)
        else:
            # Initialize a function that returns a single None or a list of
            # None values (one for each inserted column)
            if len(names) == 1:
                values = Const(None)
            else:
                values = Const([None] * len(names))
        self.values = values

    def transform(self, df):
        """Modify rows in the given data frame. Returns a modified data frame
        where columns have been inserted containing results of evaluating the
        associated value generator function.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        ValueError
        """
        # Ensure that position is valid.
        if self.pos is not None:
            if self.pos < 0 or self.pos > len(df.columns):
                raise ValueError('invalid insert position {}'.format(self.pos))
            inspos = self.pos
        else:
            inspos = len(df.columns)
        # Call the prepare method of the value generator function if it is an
        # evaluation function.
        if isinstance(self.values, EvalFunction):
            self.values.prepare(df)
        # Create a modified data frame where rows are modified by the update
        # function.
        data = list()
        # Have different implementations for single column or multi-column
        # updates.
        if len(self.names) == 1:
            for rowid, values in df.iterrows():
                val = self.values(values)
                values = list(values)
                data.append(values[:inspos] + [val] + values[inspos:])
        else:
            col_count = len(self.names)
            for rowid, values in df.iterrows():
                vals = self.values(values)
                if len(vals) != col_count:
                    msg = 'expected {} values instead of {}'
                    raise ValueError(msg.format(col_count, vals))
                if isinstance(vals, tuple):
                    vals = list(vals)
                values = list(values)
                data.append(values[:inspos] + vals + values[inspos:])
        # Insert the column names into the data frame schema.
        columns = list(df.columns)
        columns = columns[:inspos] + self.names + columns[inspos:]
        return pd.DataFrame(data=data, index=df.index, columns=columns)


class InsRow(DataFrameTransformer):
    """Data frame transformer that inserts rows into a data frame. If values
    is None a single row with all None values will be inserted. Ir values is
    a list of lists multiple rows will be inserted.
    """
    def __init__(self, pos=None, values=None):
        """Initialize the list of column names, the insert position and the
        function that is used to generate values for the inserted column(s).

        Parameters
        ----------
        pos: int, optional
            Insert position for the new columns. If None the columns will be
            appended.
        values: callable or openclean.function.base.EvalFunction, optional
            Callable that accepts a data frame row as the only argument and
            outputs a (list of) value(s) matching the number of columns
            inserted.

        Raises
        ------
        ValueError
        """
        # Ensure that names is a list
        self.pos = pos
        self.values = values

    def transform(self, df):
        """Insert rows in the given data frame. Returns a modified data frame
        where rows have been added. Raises a ValueError if the specified insert
        position is invalid or the number of values that are inserted does not
        match the schema of the given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        ValueError
        """
        # Ensure that position is valid.
        if self.pos is not None:
            if self.pos < 0 or self.pos > len(df.index):
                raise ValueError('invalid insert position {}'.format(self.pos))
            inspos = self.pos
        else:
            inspos = len(df.index)
        # Validate the inserted values
        if self.values is not None:
            if isinstance(self.values, list):
                is_lists = None
                for r in self.values:
                    if isinstance(r, list):
                        if is_lists is None:
                            is_lists = True
                        elif is_lists is False:
                            raise ValueError('invalid value list')
                        if len(r) != len(df.columns):
                            raise ValueError('invalid value list')
                    else:
                        if is_lists is None:
                            is_lists = False
                        elif is_lists is True:
                            raise ValueError('invalid value list')
                    rows = self.values
                if is_lists is False:
                    if len(self.values) != len(df.columns):
                        raise ValueError('invalid value list')
                    rows = [self.values]
            else:
                rows = [self.values] * len(df.columns)
                rows = [rows]
        # Create a modified data frame where rows are modified by the update
        # function.
        data = list()
        index = list()
        for i in range(inspos):
            data.append(list(df.iloc[i]))
            index.append(df.index[i])
        for r in rows:
            data.append(r)
            index.append(-1)
        for i in range(inspos, len(df.index)):
            data.append(list(df.iloc[i]))
            index.append(df.index[i])
        return pd.DataFrame(data=data, index=index, columns=df.columns)
