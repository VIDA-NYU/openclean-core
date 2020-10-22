# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data frame transformation operator that updates values in columns of a data
frame.
"""

import pandas as pd

from openclean.data.select import select_clause, select_by_id
from openclean.function.base import scalar_pass_through
from openclean.function.eval.base import Const, EvalFunction, Eval
from openclean.function.value.base import ValueFunction
from openclean.function.value.mapping import Lookup
from openclean.operator.base import DataFrameTransformer


# -- Functions ----------------------------------------------------------------

def update(df, columns, func):
    """Update function for data frames. Returns a modified data frame where
    values in the specified columns have been modified using the given update
    function.

    The update function is executed for each data frame row. The number of
    values returned by the function must match the number of columns that are
    being modified. Returned values are used to update column values in the
    same order as columns are specified in the columns list.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.
    func: callable or openclean.function.eval.base.EvalFunction
        Callable that accepts a data frame row as the only argument and outputs
        a (modified) list of value(s).

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return Update(columns=columns, func=func).transform(df)


def update_cell(df, colid, rowid, value):
    """Update the value for a single cell in a data frame. Cells are referenced
    by the unique column and row identifier. Raises a ValueError if an unknown
    cell is specified.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    colid: int
        Unique column identifier.
    rowid: int
        Unique row identifier
    value: scalar
        New cell value.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    # Get the index position of the specified column.
    colidx = select_by_id(df=df, colids=[colid])[0]
    # Create a modified data frame where rows are modified by the update
    # function.
    data = list()
    # Have different implementations for single column or multi-column
    # updates.
    found = False
    for rid, values in df.iterrows():
        values = list(values)
        if rid == rowid:
            values[colidx] = value
            found = True
        data.append(values)
    # Raise an error if the specified row was not found.
    if not found:
        raise ValueError("unknown row {}".format(rowid))
    return pd.DataFrame(data=data, index=df.index, columns=df.columns)


def swap(df, col1, col2):
    """Swap values in two columns of a data frame. Replaces values in column
    one with values in column two and vice versa for each row in a data frame.

    Raises a ValueError if the column arguments are not of type int or string.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    col1: int or string
        Single column index or name.
    col12: int or string
        Single column index or name.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    # Ensure that both column arguments are scalar values that specify a column
    # by its index or name.
    columns = [col1, col2]
    for c in columns:
        if not type(c) in [int, str]:
            raise ValueError('invalid column {}'.format(c))
    # Swap is a special case of the update operator

    def _swap(col_1, col_2):
        return col_2, col_1

    return update(df, columns=columns, func=_swap)


# -- Operators ----------------------------------------------------------------

class Update(DataFrameTransformer):
    """Data frame transformer that updates values in data frame column(s) using
    a given update function. The function is executed for each row and the
    resulting values replace the original cell values in the row for all listed
    columns (in their order of appearance in the columns list).
    """
    def __init__(self, columns, func):
        """Initialize the list of updated columns and the update function.

        Parameters
        ----------
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        func: callable or openclean.function.eval.base.EvalFunction
            Callable that accepts a data frame row as the only argument and
            outputs a (modified) (list of) value(s).

        Raises
        ------
        ValueError
        """
        # Ensure that columns is a list
        self.columns = columns
        self.func = get_update_function(func=func, columns=self.columns)

    def transform(self, df):
        """Modify rows in the given data frame. Returns a modified data frame
        where values have been updated by the results of evaluating the
        associated row update function.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        # Get list of indices for updated columns.
        _, colidxs = select_clause(df=df.columns, columns=self.columns)
        # Call the prepare method of the update function.
        f = self.func.prepare(df)
        # Create a modified data frame where rows are modified by the update
        # function.
        data = list()
        # Have different implementations for single column or multi-column
        # updates.
        if len(colidxs) == 1:
            colidx = colidxs[0]
            for rowid, values in df.iterrows():
                val = f.eval(values)
                values = list(values)
                values[colidx] = val
                data.append(values)
        else:
            col_count = len(colidxs)
            for rowid, values in df.iterrows():
                vals = f.eval(values)
                if len(vals) != col_count:
                    msg = 'expected {} values instead of {}'
                    raise ValueError(msg.format(col_count, vals))
                values = list(values)
                for i in range(col_count):
                    values[colidxs[i]] = vals[i]
                data.append(values)
        return pd.DataFrame(data=data, index=df.index, columns=df.columns)


# -- Helper functions ---------------------------------------------------------

def get_update_function(func, columns):
    """Helper method to ensure that the function that is passed to an update
    operator is an evaluation function that was properly initialized.

    If the function is not a callable it is converted into a constant value
    function. Special attention is given to conditional replacement functions
    that do not have their pass-through function set.

    Parameters
    ----------
    func: callable or openclean.function.eval.base.EvalFunction
        Callable that accepts a data frame row as the only argument and
        outputs a (modified) (list of) value(s).
    columns: list(int or string)
        List of column index positions or column names.

    Returns
    -------
    openclean.function.eval.base.EvalFunction
    """
    if not isinstance(func, EvalFunction):
        if isinstance(func, dict):
            func = Eval(
                columns=columns,
                func=Lookup(
                    mapping=func,
                    raise_error=False,
                    default_value=scalar_pass_through
                )
            )
        elif isinstance(func, ValueFunction) or callable(func):
            func = Eval(columns=columns, func=func)
        else:
            func = Const(func)
    return func
