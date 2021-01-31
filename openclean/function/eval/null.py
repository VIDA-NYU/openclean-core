# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test whether a given value (or list of values) is empty. A
value is defined as empty if it is None or the empty string.
"""

from openclean.function.eval.base import Eval
from openclean.function.value.null import is_empty, is_not_empty


class IsEmpty(Eval):
    """Boolean predicate that tests whether a given value or list of values
    extracted from data frame cplumns is None or the empty string.

    For any value that is not a string and not None the result should always be
    False.
    """
    def __init__(self, columns, ignore_whitespace=False):
        """Create an instance of an evaluation function that checks for empty
        values.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        ignore_whitespace: bool, optional
            Trim non-None values if the flag is set to True.
        """
        # Create instance of single value predicate.

        def func(value):
            return is_empty(value, ignore_whitespace=ignore_whitespace)

        super(IsEmpty, self).__init__(
            func=func,
            columns=columns,
            is_unary=True
        )


class IsNotEmpty(Eval):
    """Boolean predicate that tests whether a given value or list of values
    from cells in a data frame row are not None or the empty string.

    For any value that is not a string and not None the result should always be
    True.
    """
    def __init__(self, columns, ignore_whitespace=False):
        """Create an instance of an evaluation function that checks for non-
        empty values.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        ignore_whitespace: bool, optional
            Trim non-None values if the flag is set to True.
        """
        # Create instance of single value predicate.

        def func(value):
            return is_not_empty(value, ignore_whitespace=ignore_whitespace)

        super(IsNotEmpty, self).__init__(
            func=func,
            columns=columns,
            is_unary=True
        )
