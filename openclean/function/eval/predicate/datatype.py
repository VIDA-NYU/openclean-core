# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test whether a given value (or list of values) matches a
given data type constraint.
"""

from openclean.function.eval.base import All, Eval, One, is_var_func
from openclean.function.value.datatype import (
    is_datetime, is_float, is_int, is_nan
)


class IsDatetime(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values is of type date or can be converted to a date. For value
    lists the for_all flag determines whether all values have to be dates or at
    least one.
    """
    def __new__(cls, columns, formats=None, typecast=True, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are dates. The base class of the returned object depends on
        whether a single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        typecast: bool, default=True
            Attempt to parse string values as dates if True.
        formats: string or list(string)
            Date format string using Python strptime() format directives. This
            can be a list of date formats.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        def func(value):
            return is_datetime(value, formats=formats, typecast=typecast)

        if is_var_func(columns):
            if for_all:
                func = All(predicate=func)
            else:
                func = One(predicate=func)
        return Eval(func=func, columns=columns)


class IsInt(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values is of type integer or can be converted to an integer. For
    value lists the for_all flag determines whether all values have to be
    integer or at least one.
    """
    def __new__(cls, columns, typecast=True, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are integer. The base class of the returned object depends on
        whether a single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        typecast: bool, default=True
            Cast string values to integer if True.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        def func(value):
            return is_int(value, typecast=typecast)

        if is_var_func(columns):
            if for_all:
                func = All(predicate=func)
            else:
                func = One(predicate=func)
        return Eval(func=func, columns=columns)


class IsFloat(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values is of type float or can be converted to a float value.
    For value lists the for_all flag determines whether all values have to be
    floats or at least one.
    """
    def __new__(cls, columns, typecast=True, for_all=True):
        """Create an instance of an evaluation function that checks whether
        values are floats. The base class of the returned object depends on
        whether a single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        typecast: bool, default=True
            Cast string values to float if True.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        def func(value):
            return is_float(value, typecast=typecast)

        if is_var_func(columns):
            if for_all:
                func = All(predicate=func)
            else:
                func = One(predicate=func)
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
        func = is_nan
        if is_var_func(columns):
            if for_all:
                func = All(predicate=func)
            else:
                func = One(predicate=func)
        return Eval(func=func, columns=columns)
