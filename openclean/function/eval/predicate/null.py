# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test whether a given value (or list of values) is empty. A
value is defined as empty if it is None or the empty string.
"""

from openclean.function.eval.base import All, Eval, One, is_var_func
from openclean.function.value.null import is_empty, is_not_empty


class IsEmpty(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values is None or the empty string. For value lists the for_all
    flag determines whether all values have to be empty or at least one.

    For any value that is not a string and not None the result should always be
    False.
    """
    def __new__(cls, columns, for_all=True, ignore_whitespace=False):
        """Create an instance of an evaluation function that checks for empty
        values. The base class of the returned object depends on whether a
        single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        for_all: bool, optional
            Determines the 'is empty' semantics for predicates that take more
            than one argument value. If Ture, all values have to be empty.
            Otherwise, at least one value has to be empty for the predicate to
            evaluate to True.
        ignore_whitespace: bool, optional
            Trim non-None values if the flag is set to True.
        """
        def func(value):
            return is_empty(value, ignore_whitespace=ignore_whitespace)

        if is_var_func(columns):
            if for_all:
                func = All(predicate=func)
            else:
                func = One(predicate=func)
        return Eval(func=func, columns=columns)


class IsNotEmpty(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values is not None or the empty string. For value lists the
    for_all flag determines whether all values have to be non-empty or at least
    one.

    For any value that is not a string and not None the result should always be
    True.
    """
    def __new__(cls, columns, for_all=True, ignore_whitespace=False):
        """Create an instance of an evaluation function that checks for non-
        empty values. The base class of the returned object depends on whether
        a single column or a list of columns is given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        for_all: bool, optional
            Determines the 'is not empty' semantics for predicates that take
            more than one argument value. If Ture, all values have to be not
            empty. Otherwise, at least one value has to be not empty for the
            predicate to evaluate to True.
        ignore_whitespace: bool, optional
            Trim non-None values if the flag is set to True.
        """
        def func(value):
            return is_not_empty(value, ignore_whitespace=ignore_whitespace)

        if is_var_func(columns):
            if for_all:
                func = All(func)
            else:
                func = One(func)
        return Eval(func=func, columns=columns)
