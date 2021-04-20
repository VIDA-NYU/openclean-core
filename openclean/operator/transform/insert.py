# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data frame transformation operator that inserts new columns and rows into a
data frame.
"""

from typing import Callable, List, Optional, Tuple, Union

import pandas as pd

from openclean.data.stream.base import DataRow
from openclean.data.types import Scalar, DatasetSchema
from openclean.function.eval.base import Const, EvalFunction
from openclean.function.eval.base import evaluate, to_const_eval, to_eval
from openclean.operator.base import DataFrameTransformer
from openclean.operator.stream.consumer import StreamFunctionHandler
from openclean.operator.stream.processor import StreamProcessor
import numpy as np


# -- Functions ----------------------------------------------------------------

def inscol(
    df: pd.DataFrame, names: Union[str, List[str]], pos: Optional[int] = None,
    values: Optional[Union[Scalar, EvalFunction]] = None
) -> pd.DataFrame:
    """Insert function for data frame columns. Returns a modified data frame
    where columns have been inserted at a given position. Exactly one column is
    inserted for each given column name. If the insert position is undefined,
    columns are appended to the data frame. If the position does not reference
    a valid position (i.e., not between 0 and len(df.columns)) a ValueError is
    raised.

    Values for the inserted columns are generated using a given constant value
    or evaluation function. If a function is given, it is expected to return
    exactly one value (e.g., a tuple of len(names)) for each of the inserted
    columns.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame.
    names: string, or list(string)
        Names of the inserted columns.
    pos: int, default=None
        Insert position for the new columns. If None, the columns will be
        appended.
    values: scalar, tuple, or openclean.function.eval.base.EvalFunction,
            default=None
        Single value, tuple of values, or evaluation function that is used to
        generate the values for the inserted column(s). If no default is
        specified all columns will contain None.

    Returns
    -------
    pd.DataFrame
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
        Insert position for the new row(s). If None, the rows will be appended.
    values: list, optional
        List or values (to insert one row) or list of lists (to insert multiple
        rows).

    Returns
    -------
    pandas.DataFrame
    """
    return InsRow(pos=pos, values=values).transform(df)


# -- Operators ----------------------------------------------------------------

class InsCol(StreamProcessor, DataFrameTransformer):
    """Data frame transformer that inserts columns into a data frame. Values
    for the new column(s) are generated using a given value generator function.
    """
    def __init__(
        self, names: Union[str, List[str]], pos: Optional[int] = None,
        values: Optional[Union[Callable, EvalFunction, List, Scalar, Tuple]] = None
    ):
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
                openclean.function.eval.base.EvalFunction, optional
            Single value, list of constant values, callable that accepts a data
            frame row as the only argument and returns a (list of) value(s)
            matching the number of columns inserted or an evaluation function
            that returns a matchin number of values.
        """
        # Ensure that names is a list
        self.names = names if isinstance(names, list) else [names]
        self.pos = pos
        if values is not None:
            if len(self.names) > 1:
                values = to_eval(values, to_const_eval)
            else:
                values = [to_const_eval(values)]
        else:
            if len(self.names) > 1:
                values = [Const(tuple([None] * len(self.names)))]
            else:
                values = [Const(None)]
        self.values = values

    def inspos(self, schema: DatasetSchema) -> int:
        """Get the insert position for the new column.

        Raises a ValueError if the position is invalid.

        Parameters
        ----------
        schema: list of string
            Dataset input schema.

        Returns
        -------
        int
        """
        if self.pos is not None:
            if self.pos < 0 or self.pos > len(schema):
                raise ValueError('invalid insert position {}'.format(self.pos))
            return self.pos
        else:
            return len(schema)

    def open(self, schema: DatasetSchema) -> StreamFunctionHandler:
        """Factory pattern for stream consumer. Returns an instance of a
        stream consumer that re-orders values in a data stream row.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamFunctionHandler
        """
        # Get the insert position.
        inspos = self.inspos(schema)
        # Get the modified schema.
        columns = list(schema)
        columns = columns[:inspos] + self.names + columns[inspos:]

        if len(self.names) == 1:
            # Prepare the value generator.
            func = self.values[0].prepare(schema)

            def unaryfunc(row: DataRow) -> DataRow:
                """Insert column in a given data stream row."""
                values = list(row)
                return values[:inspos] + [func(row)] + values[inspos:]

            streamfunc = unaryfunc

        else:
            # Prepare the value generator.
            funcs = [f.prepare(schema) for f in self.values]
            col_count = len(self.names)

            def ternaryfunc(row: DataRow) -> DataRow:
                """Insert column values in a given data stream row."""
                # Create list of values for inserted columns. The number of
                # columns that we insert does not have to match the number of
                # evaluation functions that generate the values (i.e., we may
                # have less functions than values). We have to ensure to unpack
                # tuples or lists of values that are genrated by the evaluation
                # functions (fix for issue #64).
                insvals = []
                for f in funcs:
                    val = f(row)
                    if isinstance(val, list):
                        insvals.extend(val)
                    elif isinstance(val, tuple):
                        insvals.extend(list(val))
                    else:
                        insvals.append(val)
                # Ensure that the list of insert values matches the number of
                # inserted columns.
                values = list(row)
                if len(insvals) != col_count:
                    msg = 'expected {} values instead of {}'
                    raise ValueError(msg.format(col_count, insvals))
                return values[:inspos] + insvals + values[inspos:]

            streamfunc = ternaryfunc

        return StreamFunctionHandler(columns=columns, func=streamfunc)

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
        # Get the insert position.
        inspos = self.inspos(df.columns)
        # Evaluate the values function(s) to get the default values for the
        # inserted columns.
        defaults = evaluate(df, self.values)
        # if default is a list of tuples, transpose it
        if all(isinstance(item, tuple) for item in defaults):
            for item in defaults:
                if len(self.names) != len(item):
                    msg = 'expected {} values instead of {}'
                    raise ValueError(msg.format(len(self.names), item))
            defaults = list(map(list, zip(*defaults)))
        # it should be a regular list or series only if len(self.names) == 1
        elif not len(self.names) == 1:
            raise ValueError('expected {} values instead of 1'.format(len(self.names)))

        # Create a modified data frame where rows are modified as numpy arrays
        # this is the an optimum way to do it
        data = np.insert(df.to_numpy(copy=True), inspos, defaults, axis=1)
        # Insert the column names into the data frame schema.
        columns = list(df.columns)
        columns = columns[:inspos] + self.names + columns[inspos:]
        return pd.DataFrame(data=data, index=df.index, columns=columns, dtype=object)


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
        pos: int, default=None
            Insert position for the new columns. If None the columns will be
            appended.
        values: callable or openclean.function.eval.base.EvalFunction,
                default=None
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
                rows = [[self.values] * len(df.columns)]
        else:
            rows = [[None] * len(df.columns)]
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
        return pd.DataFrame(data=data, index=index, columns=df.columns, dtype=object)
