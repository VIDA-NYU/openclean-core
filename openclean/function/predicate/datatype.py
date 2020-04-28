# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test whether a given value (or list of values) matches a
given data type constraint.
"""

from openclean.function.base import Eval, is_var_func

import openclean.function.predicate.base as base
import openclean.function.value.datatype as vfunc


class IsDate(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values is of type date or can be converted to a date. For value
    lists the for_all flag determines whether all values have to be dates or at
    least one.
    """
    def __new__(cls, columns, format, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are dates. The base class of the returned object depends on
        whether a single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        format: string or list(string)
            Date format string using Python strptime() format directives. This
            can be a list of date formats.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        func = vfunc.is_date(format=format)
        if is_var_func(columns):
            if for_all:
                func = base.All(predicate=func)
            else:
                func = base.One(predicate=func)
        return Eval(func=func, columns=columns)


class IsInt(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values is of type integer or can be converted to an integer. For
    value lists the for_all flag determines whether all values have to be
    integer or at least one.
    """
    def __new__(cls, columns, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are integer. The base class of the returned object depends on
        whether a single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        func = vfunc.is_int()
        if is_var_func(columns):
            if for_all:
                func = base.All(predicate=func)
            else:
                func = base.One(predicate=func)
        return Eval(func=func, columns=columns)


class IsFloat(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values is of type float or can be converted to a float value.
    For value lists the for_all flag determines whether all values have to be
    floats or at least one.
    """
    def __new__(cls, columns, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are floats. The base class of the returned object depends on
        whether a single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        func = vfunc.is_float()
        if is_var_func(columns):
            if for_all:
                func = base.All(predicate=func)
            else:
                func = base.One(predicate=func)
        return Eval(func=func, columns=columns)


class IsNaN(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values are of the special type NaN (not a number). For value
    lists the for_all flag determines whether all values have to be NaN or at
    least one.
    """
    def __new__(cls, columns, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are of type NaN. The base class of the returned object depends
        on whether a single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        func = vfunc.is_nan()
        if is_var_func(columns):
            if for_all:
                func = base.All(predicate=func)
            else:
                func = base.One(predicate=func)
        return Eval(func=func, columns=columns)
