# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Class that implements the DataframeMapper abstract class to perform groupby
operations on a pandas dataframe.
"""

from openclean.data.groupby import DataFrameGrouping
from openclean.operator.base import DataGroupReducer

from typing import Optional, List, Callable, Union, Dict
import pandas as pd
from collections import defaultdict


def aggregate(groups: DataFrameGrouping, func: Union[Dict[str, Callable], Callable], schema: Optional[List[str]]=None):
    """Aggregate helper function that takes the DataFrameGouping, a schema and a function(s)
    and returns a dataframe created from the groupings using the functions following that schema


    Parameters
    ----------
    groups: DataFrameGrouping
        object returned from a GroupBy operation
    schema: list of string, optional
        list of column names
    func: (
            callable,
            dict of str:callables
        )
        If a single callable provided, it must handle the each dataframe group to create an aggregate value
        If a dict of str:callables provided, the keys are column names and the values are aggregate functions
         for each of those columns

    Returns
    -------
    pd.DataFrame
    """
    return Aggregate(schema=schema, func=func).reduce(groups=groups)


class Aggregate(DataGroupReducer):
    """Aggregate class that takes in a DataFrameGrouping and aggregate function(s),
    aggregates them and returns a dataframe
    """

    def __init__(self, func: Union[Dict[str, Callable], Callable], schema: Optional[List[str]] = None):
        """Initialize the schema and the aggregate function(s).

        Parameters
        ----------
        func: callable or dict of str:callable
            The aggregate functions
        schema: list of str
            column names of the returned dataframe
        """
        super(Aggregate, self).__init__()
        self._is_input_dict = isinstance(func, dict) # to retain memory of user input
        self.funcs = get_agg_funcs(func=func)
        self.schema = schema

    def reduce1(self, groups):
        """Reduces the groups using the agg functions and returns a dataframe

        Parameters
        ----------
        groups: DataFrameGrouping
            grouping object returned by some groupby operation

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        KeyError:
            if the input column isn't found
        Type Error:
            if the provided schema is invalid
        """
        values = defaultdict()
        for gkey, gvalue in groups.items():
            values[gkey] = defaultdict()
            for column, func in self.funcs.items():
                if callable(func):
                    if column not in gvalue.columns and self._is_input_dict:
                        raise KeyError('Aggregation column \'{}\' not found'.format(column))
                    df = gvalue[column] if column in gvalue.columns else gvalue
                    values[gkey][column] = func(df)
        df = pd.DataFrame(values).T

        if self.schema is not None:
            if len(df.columns) != len(self.schema):
                raise TypeError('Invalid schema for dataframe of size {}'.format(df.shape[1]))
            df.columns = self.schema
        return df

    def reduce(self, groups):
        single_input = not self._is_input_dict
        function = self.funcs

        result = defaultdict()
        for key, group in groups.items():
            result[key] = defaultdict()
            for col, func in function.items():
                if single_input:
                    val, single_output = is_single_or_dict(func(group))
                else:
                    if col not in group.columns:
                        raise KeyError()
                    val, single_output = is_single_or_dict(func(group[col]))

                if single_output:
                    result[key][col] = val
                else:
                    if single_input:
                        result[key] = val
                    else:
                        result[key][col] = val

        result = pd.DataFrame(result).T
        if self.schema is not None:
            if len(result.columns) != len(self.schema):
                raise TypeError('Invalid schema for dataframe of size {}'.format(result.shape[1]))
            result.columns = self.schema

        return result


# -- Helper Methods -----------------------------------------------------------
def get_agg_funcs(func):
    """Helper method used to create a mapping of the aggregation functions with their columns.

    Parameters
    ----------
    functions: dict of str:Callable or Callable
        Single Callable that aggregates on the entire df or a dict of callables
        where the keys are column names and the values are the functions for the respective column
    Returns
    -------
        dict
    """
    if not isinstance(func, dict) and callable(func):
        name = getattr(func, '__name__', repr(func))
        function = {name: func}
    elif isinstance(func, dict):
        function = func
    # todo: convert dict of funcs to dict of evals after the new eval implementation is pushed
    # elif isinstance(functions, dict):
        # for column, fun in functions.items():
        #     funcs[column] = Eval(columns=[column], func=fun)
    else:
        raise TypeError("aggregate function: {} not acceptable.".format(getattr(func, '__name__', repr(func))))

    return function


def is_single_or_dict(Y):
  if isinstance(Y, dict):
    return Y, False
  elif isinstance(Y, pd.Series):
    return Y.to_dict(), False
  elif len([Y]) == 1 and type(Y) not in (list, set, tuple, range, frozenset):
    return Y, True
  raise TypeError('func returns unacceptable type: {}'.format(type(Y)))