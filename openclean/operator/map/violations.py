# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions that return the Dataframe Violation class with violations of functional
dependencies or keys in a pandas dataframe.
"""

from collections import Counter
from typing import Callable, Dict, Optional, Union, Tuple

import pandas as pd

from openclean.data.groupby import DataFrameViolation
from openclean.function.value.base import ValueFunction
from openclean.operator.base import DataFrameMapper
from openclean.operator.map.groupby import KeyGenerator, get_eval_func


def fd_violations(
    df: pd.DataFrame, lhs: KeyGenerator, rhs: KeyGenerator
) -> DataFrameViolation:
    """ Checks for violations of a functional dependency in the given data frame.

    Parameters
    ----------
    df: pd.DataFrame
        the input pandas dataframe
    lhs: int, string, openclean.function.eval.base.EvalFunction, or list
        Generator that forms the determinant key values.
    rhs: list or str
        Generator that forms the dependant key values.

    Returns
    -------
    openclean.data.groupby.DataFrameViolation
    """

    def duplicates(meta):
        return len(meta) > 1

    fdv = Violations(lhs=lhs, rhs=rhs, func=None, having=duplicates)
    return fdv.map(df=df)


def key_violations(
    df: pd.DataFrame, columns: KeyGenerator,
    func: Optional[Union[Callable, ValueFunction]] = None, n: Optional[int] = -1
) -> DataFrameViolation:
    """Checks for violations of a key constraint in the given data frame. An
    optional func can be given to be used as a custom key generator function
    that operates on the specified columns. The optional parameter `n` can be
    used to select groups with the exact number of n violations.

    Parameters
    ----------
    df: pd.DataFrame
        the input pandas dataframe
    columns: int, string, openclean.function.eval.base.EvalFunction, or list
        Generator to extract group by keys from data frame rows.
    func: (
            openclean.function.eval.base.value.ValueFunction,
            callable,
        ), default=None
        Optional callable or value function that is used to generate a group by
        key from the values that are generate by the columns clause. This is a
        short cut to creating an evaluation function with columns as input and
        func as the evaluated function.
    n: int, default=-1
        Option to filter out groups with not exactly n violations.

    Returns
    -------
    openclean.data.groupby.DataFrameViolation
    """

    def duplicates(meta):
        if len(meta) > 1 or list(meta.values())[0] > 1:
            return True
        return False

    having = duplicates if n == -1 else n
    violations = Violations(lhs=columns, func=func, having=having)
    return violations.map(df=df)


class Violations(DataFrameMapper):
    """Violations class that:
        1) takes the left side and right side column names
        2) generates a new key from the values (func callable)
        3) identifies any tuples violating specified rules (having callable)
        4) and returns them as a DataFrameViolation object.
    """
    def __init__(self, lhs, rhs=None, func=None, having=None):
        """Initializes the Violation class with the left and right hand side
        key generators, and func and having callables.

        If no values for rhs are provided, it assumes we want to find violations
        in a singular set of column(s) (lhs).

        Parameters
        ----------
        lhs: list or string
            column name(s) of the determinant set
        rhs: string or list
            column name of the dependent set
        func: callable (Optional)
            the new key generator function
        having: int or callable (optional)
            the violation selection function (if callable, should take in a
            meta counter and return a boolean).
        """
        self.lhs = get_eval_func(columns=lhs, func=func)
        self.rhs = self.lhs if rhs is None else get_eval_func(columns=rhs)
        self.having = having

    def _transform(self, df: pd.DataFrame) -> Tuple[Dict, Dict]:
        """Groups the data frame rows and aggregates the meta data counters to
        return a dict of groups and a dict of Counters.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to transform using groupby

        Returns
        -------
        dict, dict
        """
        determinant = self.lhs.eval(df=df)
        dependent = self.rhs.eval(df=df)
        groups = dict()
        meta = dict()
        for index, values in enumerate(zip(determinant, dependent)):
            value = values[0]  # determinant value: keys
            if isinstance(value, list):
                value = tuple(value)
            if value not in groups:
                groups[value] = list()
                meta[value] = Counter()
            groups[value].append(index)

            meta_value = values[1]  # dependent value: meta
            counter = Counter([tuple(meta_value.tolist())]) if isinstance(meta_value, pd.Series) else Counter([meta_value])
            meta[value] += counter

        return groups, meta

    def map(self, df: pd.DataFrame) -> DataFrameViolation:
        """Identifies violations and maps the pandas DataFrame into a
        DataFrameViolation object.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to find violations in

        Returns
        -------
        openclean.data.groupby.DataFrameViolation
        """
        groups, meta = self._transform(df=df)
        grouping = DataFrameViolation(df=df)
        for key, rows in groups.items():
            if Violations.select(condition=self.having, meta=meta[key]):
                grouping.add(key=key, rows=rows, meta=meta[key])
        return grouping

    @staticmethod
    def select(condition: Union[Callable, int], meta: Counter) -> bool:
        """Given a dataframe and a condition, returns a bool of whether the group
        should be selected.

        Parameters
        ----------
        condition: int or callable
            if not provided, the group is selected
            if int, the group's number of rows is checked against the condition
            if callable, the meta is passed to it. The callable should return a boolean
        meta: Counter
            the meta Counter for the group/df under consideration

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
            return sum(meta.values()) == condition
        elif callable(condition):
            if not isinstance(condition(meta), bool):
                raise TypeError('selection condition expected to return a boolean')
            return condition(meta)
        return False
