# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for value generating functions. Evaluation functions are
applied to tuples (series) in a dataset (data frame). Functions are expected to
return either a scalar value or a tuple of scalar values.
"""

from openclean.function.eval.base import Eval


class Get(Eval):
    """Get value at a given index position from a list of values."""
    def __init__(self, columns, pos):
        """Initialize the object properties.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        pos: int
            Index position for the list values that is being returned.
        """

        def get_element(*args):
            """Get argument at the specified index position."""
            return args[pos]

        super(Get, self).__init__(
            func=get_element,
            columns=columns,
            is_unary=False
        )


class List(Eval):
    """Extract list of values at a given index positions from an input list of
    values.
    """
    def __init__(self, columns, positions):
        """Initialize the object properties.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        positions: list(int)
            List of index position for the extracted list values.
        """

        def get_list(*args):
            """Get argument at the specified index position."""
            return [args[i] for i in positions]

        super(List, self).__init__(
            func=get_list,
            columns=columns,
            is_unary=False
        )
