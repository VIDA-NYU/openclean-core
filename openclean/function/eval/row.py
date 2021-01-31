# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of evaluation functions that compute a result over a list of
values that are extracted for data frame rows.
"""

from openclean.function.eval.base import Eval


class Greatest(Eval):
    """Evaluation function that returns the maximum value for a list of values
    from different cells in a data frame row.
    """
    def __init__(self, *args):
        """Initialize the list of value producers.

        Parameters
        ----------
        args: list of openclean.function.eval.base.EvalFunction
            List of predicates (evaluation functions).
        """

        def eval(values):
            return max(values, default=None)

        super(Greatest, self).__init__(columns=args, func=eval, is_unary=True)


class Least(Eval):
    """Evaluation function that returns the minimum of values for one or more
    columns in a data frame as the result value for all columns in the data
    frame.
    """
    def __init__(self, *args):
        """Initialize the list of value producers.

        Parameters
        ----------
        args: list of openclean.function.eval.base.EvalFunction
            List of predicates (evaluation functions).
        """

        def eval(values):
            return min(values, default=None)

        super(Least, self).__init__(columns=args, func=eval, is_unary=True)
