# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test whether a given value (or list of values) match a
regular expression.
"""

from openclean.function.base import Eval, is_var_func

import openclean.function.predicate.base as base
import openclean.function.value.regex as vfunc


class IsMatch(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values match a regular expression. For value lists the for_all
    flag determines whether all values have to match the expression or at least
    one of them.
    """
    def __new__(
        cls, columns, pattern, fullmatch=False, as_string=True, for_all=True
    ):
        """Create an instance of an evaluation function that checks whether
        a value matches a regular expression. The base class of the returned
        object depends on whether a single column or a list of columns is
        given.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        pattern: string
            Regular expression.
        fullmatch: bool, optional
            If True, the pattern has to match a given string fully in order for
            the predicate to evaluate to True.
        as_string: bool, optional
            Convert values that are not of type string to string if True.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        func = vfunc.is_match(
            pattern=pattern,
            fullmatch=fullmatch,
            as_string=as_string
        )
        if is_var_func(columns):
            if for_all:
                func = base.All(predicate=func)
            else:
                func = base.One(predicate=func)
        return Eval(func=func, columns=columns)


class IsNotMatch(object):
    """Factory pattern for Boolean predicates that tests whether a given value
    or list of values don't match a regular expression. For value lists the
    for_all flag determines whether all values have to match the expression or
    at least one of them.
    """
    def __new__(
        cls, columns, pattern, fullmatch=False, as_string=True, for_all=True
    ):
        """Create an instance of a single column or multi-column predicate.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        pattern: string
            Regular expression.
        fullmatch: bool, optional
            If True, the pattern has to match a given string fully in order for
            the predicate to evaluate to True.
        as_string: bool, optional
            Convert values that are not of type string to string if True.
        for_all: bool, optional
            Determines semantics for predicates that take more than one
            argument value. If Ture, all values have to evaluate to True.
            Otherwise, at least one value has to evaluate to True.
        """
        func = vfunc.is_not_match(
            pattern=pattern,
            fullmatch=fullmatch,
            as_string=as_string
        )
        if is_var_func(columns):
            if for_all:
                func = base.All(func)
            else:
                func = base.One(func)
        return Eval(func=func, columns=columns)
