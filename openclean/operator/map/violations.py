# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""
functions that return the Dataframe Grouping class with violations of functional dependencies or keys in a pandas dataframe.
"""
from openclean.data.groupby import DataFrameGrouping
from openclean.operator.map.groupby import groupby
from openclean.function.distinct import distinct


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
        DataFrameGrouping
        """
    def duplicates(group):
        return len(distinct(group, rhs)) > 1

    violations = groupby(df=df, columns=lhs, func=None, having=duplicates)
    return violations


def key_violations(df, columns, func=None):
    """ Checks for any key violations in df.
        An optional func can be given to use a custom key generator func using the provided columns

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

        Returns
        -------
        DataFrameGrouping
        """

    def duplicates(group):
        return group.shape[0] > 1

    violations = groupby(df=df, columns=columns, func=func, having=duplicates)
    return violations


def n_violations(df, columns, func=None, n=1):
    """ Checks for any key violations in df and selects groups with the exact number of n violations
        An optional func can be given to use a custom key generator func using the provided columns

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
        DataFrameGrouping
        """

    violations = groupby(df=df, columns=columns, func=func, having=n)
    return violations
