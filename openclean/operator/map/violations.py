# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""
functions that return the Dataframe Violation class with violations of functional dependencies or keys in a pandas dataframe.
"""
from openclean.data.groupby import DataFrameViolation
from openclean.operator.base import DataFrameMapper
from collections import Counter
from openclean.operator.map.groupby import get_eval_func
import pandas as pd


def fd_violations(df, lhs, rhs):
    """ Checks for any FD violations in df.

        Parameters
        ----------
        df: pd.DataFrame
            the input pandas dataframe
        lhs: list or str
            lhs column(s) that form the determinant set
        rhs: list or str
            the dependant column(s)

        Returns
        -------
        DataFrameViolation
        """
    def duplicates(meta):
        return len(meta) > 1

    fdv = Violations(lhs=lhs, rhs=rhs, func=None, having=duplicates)
    return fdv.map(df=df)

def key_violations(df, columns, func=None, n=-1):
    """ Checks for any key violations in df.
        An optional func can be given to use a custom key generator func using the provided columns
        n can be specified to select groups with the exact number of n violations

        Parameters
        ----------
        df: pd.DataFrame
            the input pandas dataframe
        columns: list or str
            column(s) to check for key violations.
            Note: if a func is provided, it wont check those individual columns for violations
             - instead it will generate a new key using the func first
        func: optional callable
            to provide to the underlying groupby as the new key generator operation
        n: int
            filters out groups with not exactly n violations

        Returns
        -------
        DataFrameViolation
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
        3) identifies any tuples violating specified rules  (having callable)
        4) and returns them as a DataFrameViolation object"""
    def __init__(self, lhs, rhs=None, func=None, having=None):
        """
        Initializes the Violation class with the left and right hand side column names, and func and having callables.
        If no values for rhs are provided, it assumes we want to find violations in a singular set of column(s) (lhs)

        Parameters
        ----------
        lhs: list or string
            column name(s) of the determinant set
        rhs: string
            column name of the dependent set
        func: callable (Optional)
            the new key generator function
        having: int or callable (optional)
            the violation selection function (if callable, should take in a meta counter and return a boolean)
        """
        self.lhs = lhs
        self.rhs = lhs if rhs is None else rhs
        self.func = get_eval_func(columns=lhs, func=func)
        self.having = having

    def _transform(self, df):
        """Groups the df and aggregates the meta data counters to return a dict of groups and a dict of Counters.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to transform using groupby

        Returns
        -------
        dict, dict
        """
        evaluated = self.func.eval(df=df)
        groups = dict()
        meta = dict()
        for index, value in enumerate(evaluated):
            if isinstance(value, list):
                value = tuple(value)
            if value not in groups:
                groups[value] = list()
                meta[value] = Counter()
            groups[value].append(index)

            meta_value = df.loc[index, self.rhs]
            meta[value] += Counter([tuple(meta_value.tolist())]) if isinstance(meta_value, pd.Series) else Counter([meta_value])

        return groups, meta

    def map(self, df):
        """Identifies violations and maps the pandas DataFrame into a DataFrameViolation object.

        Parameters
        ----------
        df: pandas.DataFrame
            Dataframe to find violations in

        Returns
        -------
        DataFrameViolation
        """
        df_reindexed = df.reset_index(drop=isinstance(df.index, pd.RangeIndex))
        groups, meta = self._transform(df=df_reindexed)
        grouping = DataFrameViolation(df=df_reindexed, lhs=self.lhs, rhs=self.rhs)
        for key, rows in groups.items():
            if Violations.select(condition=self.having, meta=meta[key]):
                grouping.add(key=key, rows=rows, meta=meta[key])
        return grouping

    @staticmethod
    def select(condition, meta):
        """
        Given a dataframe and a condition, returns a bool of whether the group should be selected

        Parameters
        ----------
        meta: Counter
            the meta Counter for the group/df under consideration
        condition: int or callable
            if not provided, the group is selected
            if int, the group's number of rows is checked against the condition
            if callable, the meta is passed to it. The callable should return a boolean

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
