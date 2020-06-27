# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test whether a regular expression matches a given input
string.
"""

from openclean.function.value.base import PreparedFunction

import openclean.util as util


def replace(predicate, value):
    """Return an instance of the Replace class for the given arguments.

    Paramaters
    ----------
    predicate: callable
        Predicate that is evalauated on input values.
    value: scalar or tuple
        Replacement value for inputs that satisfy the predicate.

    Returns
    -------
    openclean.function.value.replace.Replace
    """
    return Replace(predicate=predicate, value=value)


class Replace(PreparedFunction):
    """Replace funciton that replaces values matching a given predicate with a
    predefined value.
    """
    def __init__(self, predicate, value):
        """Initialize the predicate and the replacement value.

        Parameters
        ----------
        predicate: callable
            Predicate that is evalauated on input values.
        value: scalar or tuple
            Replacement value for inputs that satisfy the predicate.
        """
        super(Replace, self).__init__(name='replace')
        self.predicate = util.ensure_callable(predicate)
        self.value = value

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
        if self.predicate(value):
            return self.value
        else:
            return value
