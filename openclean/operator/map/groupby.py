# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Class that implements the DataframeMapper abstract class to perform groupby
operations on a pandas dataframe.
"""

from typing import Callable, Dict, List, Optional, Union

import pandas as pd

from openclean.data.groupby import DataFrameGrouping
from openclean.function.eval.base import EvalFunction, Eval, InputColumn
from openclean.function.value.base import ValueFunction
from openclean.operator.base import DataFrameMapper


"""Type aliases for group by keys. Keys are either scalar values or lists of
values. The individual values are either extracted from data frame columns or
are generated by evaluation functions.
"""
KeyGenerator = Union[InputColumn, List[InputColumn]]


def groupby(
    df: pd.DataFrame, columns: KeyGenerator,
    func: Optional[Union[Callable, ValueFunction]] = None,
    having: Optional[Union[Callable, int]] = None
) -> DataFrameGrouping:
    """Groupby function for data frames. Evaluates a new index based on the
    rows of the dataframe using the input function (optional). The output
    comprises of a openclean.data.groupby.DataFrameGrouping object.


    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, openclean.function.eval.base.EvalFunction, or list, default=None
        Single column or evaluation function or a list of columns or evaluation
        functions. The column(s)/function(s) are used to genrate group by keys
        for each row in the input data frame.
    func: (
            openclean.function.eval.base.value.ValueFunction,
            callable,
        ), default=None
        Optional callable or value function that is used to generate a group by
        key from the values that are generate by the columns clause. This is a
        short cut to creating an evaluation function with columns as input and
        func as the evaluated function.
    having: int or callable, default=None
        If given, group by only returns groups that (i) have a number of rows
        that equals a given int or (ii) (if a callable is given) we pass the
        group to that callable as an argument and if the returned result is
        True the group is included in the returned result. The callable should
        expect a pandas dataframe and return a boolean.

    Returns
    -------
    openclean.data.groupby.DataFrameGrouping
    """
    gpby = GroupBy(columns=columns, func=func)
    all_groups = gpby.map(df=df)

    if having is not None:
        selected_groups = DataFrameGrouping(df=df)
        for key, group in all_groups.groups():
            if GroupBy.select(group, having):
                selected_groups.add(key=key, rows=all_groups.rows(key=key))
    else:
        selected_groups = all_groups

    return selected_groups


class GroupBy(DataFrameMapper):
    """GroupBy class that takes in the column names to group on and a function
    (optional), performs the groupby and returns a DataFrameGrouping object.
    """
    def __init__(
        self, columns: KeyGenerator, func: Optional[Union[Callable, ValueFunction]] = None
    ):
        """Initialize the column names and an optional function.

        Parameters
        ----------
        columns: int, string, openclean.function.eval.base.EvalFunction, or list
            Single column or evaluation function or a list of columns or evaluation
            functions. The column(s)/function(s) are used to genrate group by keys
            for rows in a input data frame.
        func: (
                openclean.function.eval.base.value.ValueFunction,
                callable,
            ), default=None
            Optional callable or value function that is used to generate a group by
            key from the values that are generate by the columns clause. This is a
            short cut to creating an evaluation function with columns as input and
            func as the evaluated function.
        """
        super(GroupBy, self).__init__()
        self.func = get_eval_func(columns=columns, func=func)

    def _transform(self, df: pd.DataFrame) -> Dict:
        """Applies the groupby function and returns a dict of groups.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to transform using groupby

        Returns
        _______
        dict
        """
        evaluated = self.func.eval(df=df)
        groups = dict()
        for index, value in enumerate(evaluated):
            if isinstance(value, list):
                value = tuple(value)
            if value not in groups:
                groups[value] = list()
            groups[value].append(index)

        return groups

    def map(self, df: pd.DataFrame) -> DataFrameGrouping:
        """transforms and maps a pandas DataFrame into a DataFrameGrouping object.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to transform using groupby

        Returns
        _______
        openclean.data.groupby.DataFrameGrouping
        """
        groupedby = self._transform(df=df)
        grouping = DataFrameGrouping(df=df)
        for gby in groupedby:
            grouping.add(key=gby, rows=groupedby[gby])
        return grouping

    @staticmethod
    def select(group: pd.DataFrame, condition: Union[Callable, int]) -> bool:
        """Given a dataframe and a condition, returns a bool of whether the
        group should be selected.

        Parameters
        ----------
        group: pd.DataFrame
            the group/df under consideration
        condition: int or callable
            if not provided, the group is selected
            if int, the group's number of rows is checked against the condition
            if callable, the group is passed to it. The callable should return a boolean

        Returns
        -------
        bool

        Raises
        ------
        TypeError
        """
        if condition is None:
            return True
        elif isinstance(condition, int):
            return group.shape[0] == condition
        elif callable(condition):
            if not isinstance(condition(group), bool):
                raise TypeError('selection condition expected to return a boolean')
            return condition(group)
        return False


# -- Helper Functions ---------------------------------------------------------

def get_eval_func(
    columns: KeyGenerator, func: Optional[Union[Callable, ValueFunction]] = None
) -> EvalFunction:
    """Helper function used to create an evaluation function from a key generator
    specification.

    Parameters
    ----------
    columns: int, string, openclean.function.eval.base.EvalFunction, or list
        Single column or evaluation function or a list of columns or evaluation
        functions. The column(s)/function(s) are used to genrate group by keys
        for each row in the input data frame.
    func: (
            openclean.function.eval.base.value.ValueFunction,
            callable,
        ), default=None
        Optional callable or value function that is used to generate a group by
        key from the values that are generate by the columns clause. This is a
        short cut to creating an evaluation function with columns as input and
        func as the evaluated function.

    Returns
    -------
    openclean.function.eval.base.EvalFunction
    """
    # If func is given we return an evaluation function that operates on the
    # given columns.
    if func is not None or not isinstance(columns, EvalFunction):
        return Eval(columns=columns, func=func)
    return columns
