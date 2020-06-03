# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Conditional value functions. These functions represent if-then-else
statements. they evaluate a given predicate for each value and then return
either of to values.
"""

from openclean.function.value.base import (
    CallableWrapper, ValueFunction, scalar_pass_through, to_valuefunc
)


# -- Generic compare operator -------------------------------------------------

class IfThenElse(ValueFunction):
    """Conditional value function that implements an if-then-else statement.
    The function evaluates a given predicate for each value. Depending on the
    evaluation result either the associated if-expression is evalauted on the
    value or the else-expression.
    """
    def __init__(self, predicate, if_expression, else_expression=None):
        """Initialize the associated predicate and conditional expressions for
        the if and else block.

        Parameters
        ----------
        predicate: (
                scalar,
                callable, or
                openclean.function.value.base.ValueFunction
            )
            Predicate that represents the condition of the if-them-else
            statement.
        if_expression: (
                scalar,
                callable, or
                openclean.function.value.base.ValueFunction
            )
            If-expression that is evaluated for all values that satisfy the
            predicate.
        else_expression: (
                scalar,
                callable, or
                openclean.function.value.base.ValueFunction
            )
            Else-expression that is evaluated for all values that do not
            satisfy the predicate.
        """
        self.predicate = to_valuefunc(predicate)
        self.if_expression = to_valuefunc(if_expression)
        if else_expression is not None:
            self.else_expression = to_valuefunc(else_expression)
        else:
            self.else_expression = CallableWrapper(scalar_pass_through)

    def eval(self, value):
        """Evaluate the condition on the given value. Depending on the result,
        return either of the if or else expression result for the value.

        Parameters
        ----------
        value: scalar
            Scalar value that is being tested and transformed.

        Returns
        -------
        scalar or tuple
        """
        if self.predicate(value):
            return self.if_expression.eval(value)
        else:
            return self.else_expression.eval(value)

    __call__ = eval

    def is_prepared(self):
        """Returns False if either of the ssociated value functions requires
        preparation.

        Returns
        -------
        bool
        """
        for f in [self.predicate, self.if_expression, self.else_expression]:
            if not f.is_prepared():
                return False
        return True

    def prepare(self, values):
        """Prepare the associated value functions.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        openclean.function.value.base.ValueFunction
        """
        if not self.predicate.is_prepared():
            self.predicate.prepate(values)
        if not self.if_expression.is_prepared():
            self.if_expression.prepare(values)
        if not self.else_expression.is_prepared():
            self.else_expression.prepare(values)
        return self


class IfThen(IfThenElse):
    """Short-cut for a function that has no else clause. For values that do not
    satisy the predicate the value is returned as is.
    """
    def __init__(self, predicate, if_expression):
        """Initialize the associated predicate and the if-expression.

        Parameters
        ----------
        predicate: (
                scalar,
                callable, or
                openclean.function.value.base.ValueFunction
            )
            Predicate that represents the condition of the if-them-else
            statement.
        if_expression: (
                scalar,
                callable, or
                openclean.function.value.base.ValueFunction
            )
            If-expression that is evaluated for all values that satisfy the
            predicate.
        else_expression: (
                scalar,
                callable, or
                openclean.function.value.base.ValueFunction
            )
            Else-expression that is evaluated for all values that do not
            satisfy the predicate.
        """
        super(IfThen, self).__init__(
            predicate=predicate,
            if_expression=if_expression
        )


class Replace(IfThen):
    """Synonym for a if-then function that is used as conditional replace
    function.
    """
    def __init__(self, condition, value):
        """Initialize the associated predicate and the if-expression.

        Parameters
        ----------
        condition: (
                scalar,
                callable, or
                openclean.function.value.base.ValueFunction
            )
            Predicate that represents the condition of the if-them-else
            statement.
        value: (
                scalar,
                callable, or
                openclean.function.value.base.ValueFunction
            )
            Expression that is evaluated for all values that satisfy the
            condition.
        """
        super(Replace, self).__init__(
            predicate=condition,
            if_expression=value
        )
