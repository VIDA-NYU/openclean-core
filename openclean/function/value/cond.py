# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""The mapping operator that returns a dictionary that contains a mapping of
original values in a data frame column(s) to results of applying a given value
function on them.

Lookup functions represent mappings using dictionaries.
"""

from openclean.function.value.base import PreparedFunction, to_value_function


class ConditionalStatement(PreparedFunction):
    """Conditional if-then-else statement. Depending on a given predicate
    either one of two statements is evaluated.
    """
    def __init__(self, predicate, stmt, elsestmt=None):
        """Initialize the predicate and the replacement value.

        Parameters
        ----------
        predicate: callable or scalar
            Predicate that is evaluated on the input values.
        stmt: callable or scalar
            Statement that is evaluated for values that satisfy the predicate.
        elsestmt: callable or scalar, default=None
            Statement that is evaluated for values that do not satisfy the
            predicate.
        """
        self.predicate = to_value_function(predicate)
        self.stmt = to_value_function(stmt)
        self.elsestmt = to_value_function(elsestmt)

    def eval(self, value):
        """Replace function returns the predefined replacement value if the
        given value satisfies the predicate. Otherwise, the argument value is
        returned.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        any
        """
        if self.predicate.eval(value):
            return self.stmt.eval(value)
        else:
            return self.elsestmt.eval(value)
