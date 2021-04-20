# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data frame transformation operator that updates values in columns of a data
frame.
"""

from typing import Callable, Dict, Union

import pandas as pd

from openclean.data.stream.base import DataRow
from openclean.data.schema import select_clause
from openclean.data.types import ColumnRef, Columns, Scalar, DatasetSchema
from openclean.function.eval.base import Const, EvalFunction, Eval
from openclean.function.eval.domain import Lookup
from openclean.function.value.base import ValueFunction
from openclean.operator.base import DataFrameTransformer
from openclean.operator.stream.consumer import StreamFunctionHandler
from openclean.operator.stream.processor import StreamProcessor

"""Type alias for update function specifications."""
UpdateFunction = Union[Callable, Dict, EvalFunction, Scalar, ValueFunction]


# -- Functions ----------------------------------------------------------------

def update(df: pd.DataFrame, columns: Columns, func: UpdateFunction) -> pd.DataFrame:
    """Update function for data frames. Returns a modified data frame where
    values in the specified columns have been modified using the given update
    function.

    The update function is executed for each data frame row. The number of
    values returned by the function must match the number of columns that are
    being modified. Returned values are used to update column values in the
    same order as columns are specified in the columns list.

    The function that is used to generate the update values will be a evaluation
    function. The user has the option to also provide a constant value, a lookup
    dictionary, or a callable (or value function) that accepts a single value.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame.
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.
    func: scalar, dict, callable, openclean.function.value.base.ValueFunction,
            or openclean.function.eval.base.EvalFunction
        Specification of the (resulting) evaluation function that is used to
        generate the updated values for each row in the data frame.

    Returns
    -------
    pd.DataFrame
    """
    return Update(columns=columns, func=func).transform(df)


def swap(df: pd.DataFrame, col1: ColumnRef, col2: ColumnRef) -> pd.DataFrame:
    """Swap values in two columns of a data frame. Replaces values in column
    one with values in column two and vice versa for each row in a data frame.

    Raises a ValueError if the column arguments are not of type int or string.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame.
    col1: int or string
        Single column index or name.
    col12: int or string
        Single column index or name.

    Returns
    -------
    pd.DataFrame

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

class Update(StreamProcessor, DataFrameTransformer):
    """Data frame transformer that updates values in data frame column(s) using
    a given update function. The function is executed for each row and the
    resulting values replace the original cell values in the row for all listed
    columns (in their order of appearance in the columns list).
    """
    def __init__(self, columns: Columns, func: UpdateFunction):
        """Initialize the list of updated columns and the update function.

        Parameters
        ----------
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        func: scalar, dict, callable, openclean.function.value.base.ValueFunction,
            or openclean.function.eval.base.EvalFunction
            Specification of the (resulting) evaluation function that is used to
            generate the updated values for each row in the data frame.

        Raises
        ------
        ValueError
        """
        # Ensure that columns is a list
        self.columns = columns
        self.func = get_update_function(func=func, columns=self.columns)

    def open(self, schema: DatasetSchema) -> StreamFunctionHandler:
        """Factory pattern for stream consumer. Returns an instance of a
        stream consumer that updates values in a data stream row.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamFunctionHandler
        """
        # Get the index positions for the updated column(s).
        _, colidxs = select_clause(schema=schema, columns=self.columns)
        # Get the stream function that updates the values in data stream rows.
        func = self.func.prepare(columns=schema)

        def updfunc(row: DataRow) -> DataRow:
            """Update columns in a data stream row using func."""
            val = func(row)
            values = list(row)
            if len(colidxs) == 1:
                values[colidxs[0]] = val
            else:
                if len(val) != len(colidxs):
                    msg = 'expected {} values instead of {}'
                    raise ValueError(msg.format(len(colidxs), len(val)))
                for i, col in enumerate(colidxs):
                    values[col] = val[i]
            return values

        return StreamFunctionHandler(columns=schema, func=updfunc)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
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
        _, colidxs = select_clause(schema=df.columns, columns=self.columns)
        # Evaluate the update function to get the modified values for the
        # updated columns.
        updates = self.func.eval(df)
        # Create a modified data frame where rows are modified by the update
        # function.
        if all(isinstance(item, tuple) for item in updates):
            for item in updates:
                if len(colidxs) != len(item):
                    msg = 'expected {} values instead of {}'
                    raise ValueError(msg.format(len(colidxs), item))
        # it should be a regular list or series only if len(self.names) == 1
        elif not len(colidxs) == 1:
            raise ValueError('expected {} values instead of 1'.format(len(colidxs)))
        # if single column updates, convert the updates list to a list of lists (vector) for numpy
        else:
            updates = list(map(list, zip(updates)))

        data = df.to_numpy(copy=True)
        data[:, colidxs] = updates
        return pd.DataFrame(data=data, index=df.index, columns=df.columns, dtype=object)


# -- Helper functions ---------------------------------------------------------

def get_update_function(
    func: UpdateFunction, columns: Columns
) -> EvalFunction:
    """Helper method to ensure that the function that is passed to an update
    operator is an evaluation function that was properly initialized.

    If the function argument is a dictionary it is converted into a lookup
    table. if the argument is a scalar value it is converted into a constant
    evaluation function. Special attention is given to conditional replacement functions
    that do not have their pass-through function set.

    Parameters
    ----------
    func: scalar, dict, callable, openclean.function.value.base.ValueFunction,
            or openclean.function.eval.base.EvalFunction
        Specification of the (resulting) evaluation function that is used to
        generate the updated values for each row in the data frame.
    columns: list(int or string)
        List of column index positions or column names.

    Returns
    -------
    openclean.function.eval.base.EvalFunction
    """
    if not isinstance(func, EvalFunction):
        if isinstance(func, dict):
            return Lookup(columns=columns, mapping=func)
        elif isinstance(func, ValueFunction) or callable(func):
            return Eval(columns=columns, func=func)
        else:
            return Const(func)
    return func
