# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Apply operator for data frames."""

import pandas as pd

from openclean.data.sequence import Sequence
from openclean.data.schema import as_list, select_clause
from openclean.function.value.base import CallableWrapper, ValueFunction
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
    func: callable or openclean.function.eval.base.ApplyFactory
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
        func: (openclean.function.eval.base.ValueFunction, or callable)
            Callable or value function that accepts a single value.
        """
        # Ensure that columns is a list.
        self.columns = as_list(columns)
        # Ensure that the function is a value function.
        if not isinstance(func, ValueFunction):
            # Instantiate the function if a class object is given
            if isinstance(func, type):
                func = func()
            func = CallableWrapper(func=func)
        self.func = func

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
        _, colidxs = select_clause(schema=df.columns, columns=self.columns)
        # Apply the value function to each column separately.
        functions = list()
        for colidx in colidxs:
            if not self.func.is_prepared():
                f = self.func.prepare(Sequence(df, colidx))
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
        return pd.DataFrame(data=data, index=df.index, columns=df.columns, dtype=object)
