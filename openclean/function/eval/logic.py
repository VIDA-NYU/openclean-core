# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Implementation of basic logic operators as evaluation function predicates
for data frame rows.
"""

from openclean.function.eval.base import Eval


class And(Eval):
    """Logical conjunction of predicates."""
    def __init__(self, *args):
        """Initialize the list of predicates in the conjunction.

        Parameters
        ----------
        args: list of openclean.function.eval.base.EvalFunction
            List of predicates (evaluation functions).
        """

        def eval(values):
            for v in values:
                if not v:
                    return False
            return True

        super(And, self).__init__(columns=args, func=eval, is_unary=True)


class Not(Eval):
    """Logical negation of a predicate(s)."""
    def __init__(self, predicate):
        """Initialize the predicate that is being negated. Raises a ValueError
        if more than one predicate is given.

        Parameters
        ----------
        predicate: openclean.function.eval.base.EvalFunction
            Single evaluation function.
        """

        def eval(values):
            return not values

        super(Not, self).__init__(columns=predicate, func=eval, is_unary=True)


class Or(Eval):
    """Logical disjunction of predicates."""
    def __init__(self, *args):
        """Initialize the list of predicates in the disjunction.

        Parameters
        ----------
        args: list of openclean.function.eval.base.EvalFunction
            List of predicates (evaluation functions).
        """
        def eval(values):
            for v in values:
                if v:
                    return True
            return False

        super(Or, self).__init__(columns=args, func=eval, is_unary=True)
