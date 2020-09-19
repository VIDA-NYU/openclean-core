# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Class that implements the DataframeMapper abstract class to perform groupby operations on a pandas dataframe."""

from openclean.data.groupby import DataFrameGrouping
from openclean.operator.base import DataFrameMapper
from openclean.function.eval.base import (
    Col, EvalFunction, CallableWrapper, Eval
)


def groupby(df, columns, func=None):
    """Groupby function for data frames. Evaluates a new index based on the rows of the dataframe
        using the input function (optional). The output comprises of a openclean.data.groupby.DataFrameGrouping
        object.


        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        func: (
                openclean.function.eval.base.EvalFunction,
                openclean.function.eval.base.value.ValueFunction,
                callable,
            )
            Evaluation function or callable that accepts a data frame row as the
            only argument (if columns is None). ValueFunction or callable if one
            or more columns are specified.

        Returns
        -------
        openclean.data.groupby.DataFrameGrouping
        """
    gpby = GroupBy(columns=columns, func=func)
    return gpby.map(df=df)


class GroupBy(DataFrameMapper):
    """GroupBy class that takes in the column names to group on and a function (optional),
    performs the groupby and returns a DataFrameGrouping object"""
    def __init__(self, columns, func=None):
        """Initialize the column names and an optional function.

        Parameters
        ----------
        columns: list or string
            The column names to group by on
        func: callable
            The new index generator function
        """
        super(GroupBy, self).__init__()
        self.func = get_eval_func(columns=columns, func=func)

    def _transform(self, df):
        """Applies the groupby function and returns a dict of groups.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to transform using groupby

        Returns
        _______
        dict
        """
        prepared = self.func.prepare(df=df)
        groups = dict()
        for index, rows in df.iterrows():
            value = prepared.eval(rows)
            if isinstance(value, list):
                value = tuple(value)
            if value not in groups:
                groups[value] = list()
            groups[value].append(index)

        return groups

    def map(self, df):
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


# -- Helper Methods -----------------------------------------------------------
def get_eval_func(columns=None, func=None):
    """Helper method used to evaluate a func on data frame rows.

    Parameters
    ----------
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.
    func: (
            openclean.function.eval.base.EvalFunction,
            openclean.function.eval.base.value.ValueFunction,
            callable,
        )
        Evaluation function or callable that accepts a data frame row as the
        only argument (if columns is None). ValueFunction or callable if one
        or more columns are specified.

    Returns
    -------
    openclean.function.eval.base.EvalFunction

    Raises
    ------
    ValueError
    """
    # Raise a ValueError if no useful inputs found.
    if func is None and columns is None:
        raise ValueError('missing inputs')

    # If columns is a callable or eval function and func is None we
    # flip the columns and func values.
    if func is None and columns is not None:
        if isinstance(columns, EvalFunction):
            func = columns
            columns = None
        else:
            columns = [columns] if not isinstance(columns, list) else columns
            func = Col(columns=columns)
        return func

    # If one or more columns and func both are specified
    if columns is not None:
        # Ensure that columns is a list.
        if not isinstance(columns, list):
            columns = [columns]
        # Convert func to an evaluation function.
        if callable(func):
            func = Eval(func=CallableWrapper(func=func), columns=columns)

    elif not isinstance(func, EvalFunction):
        func = Eval(func=func, columns=columns)
    return func