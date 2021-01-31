# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test whether a given value (or list of values) match a
regular expression.
"""

from openclean.function.base import All, One
from openclean.function.eval.base import Eval
from openclean.function.value.regex import IsMatch as is_match
from openclean.function.value.regex import IsNotMatch as is_not_match


class IsMatch(Eval):
    """Boolean predicate that tests whether a given value or list of values
    from a data frame row match a regular expression. For value lists the for
    all flag determines whether all values have to match the expression or at
    least one of them.
    """
    def __init__(
        self, columns, pattern, fullmatch=False, as_string=True, for_all=True
    ):
        """Create an instance of an evaluation function that checks whether
        a value matches a regular expression.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
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
        func = is_match(
            pattern=pattern,
            fullmatch=fullmatch,
            as_string=as_string
        )
        if for_all:
            func = All(predicate=func)
        else:
            func = One(predicate=func)
        super(IsMatch, self).__init__(columns=columns, func=func)


class IsNotMatch(Eval):
    """Boolean predicate that tests whether a given value or list of values
    from a data frame row don't match a regular expression. For value lists the
    for all flag determines whether all values have to match the expression or
    at least one of them.
    """
    def __init__(
        self, columns, pattern, fullmatch=False, as_string=True, for_all=True
    ):
        """Create an instance of a single column or multi-column predicate.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
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
        func = is_not_match(
            pattern=pattern,
            fullmatch=fullmatch,
            as_string=as_string
        )
        if for_all:
            func = All(func)
        else:
            func = One(func)
        super(IsNotMatch, self).__init__(columns=columns, func=func)
