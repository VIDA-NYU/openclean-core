# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Apply operator for data frames."""

import pandas as pd

from openclean.data.column import as_list, select_clause
from openclean.function.base import ApplyFactory
from openclean.operator.base import DataFrameTransformer


def apply(df, columns, func):
    """Apply function for data frames. Returns a modified data frame where
    values in the specified columns have been modified using the given apply
    function.

    The apply function can be an apply factory. In this case, a separate
    instance of the function is generated and applied to each column.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.
    func: callable or openclean.function.base.ApplyFactory
        Callable that accepts a single value or a fatory that creates such a
        callable.

    Returns
    -------
    pandas.DataFrame
    """
    return Apply(columns=columns, func=func).transform(df)


class Apply(DataFrameTransformer):
    """Apply function for data frames. Returns a modified data frame where
    values in the specified columns have been modified using the given apply
    function.

    The apply function can be an apply factory. In this case, a separate
    instance of the function is generated and applied to each column.
    """
    def __init__(self, columns, func):
        """Initialize the list of column and the apply function.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        func: callable or openclean.function.base.ApplyFactory
            Callable that accepts a single value or a fatory that creates such
            a callable.
        """
        # Ensure that columns is a list.
        self.columns = as_list(columns)
        # Instantiate the function if a class object is given
        self.func = func() if isinstance(func, type) else func

    def transform(self, df):
        """Return a data frame where all values in the specified columns have
        been modified by the apply function.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        # Get list of indices for updated columns.
        _, colidxs = select_clause(df=df, columns=self.columns)
        # Create apply functions for all columns.
        functions = list()
        for colidx in colidxs:
            if isinstance(self.func, ApplyFactory):
                f = self.func.get_func(df, colidx)
            else:
                f = self.func
            functions.append((f, colidx))
        # Create modified data frame.
        data = list()
        for _, values in df.iterrows():
            values = list(values)
            for f, colidx in functions:
                values[colidx] = f(values[colidx])
            data.append(values)
        return pd.DataFrame(data=data, index=df.index, columns=df.columns)
