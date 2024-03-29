# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper functions for various purpoeses."""

from typing import Optional

import uuid


def always_false(*args):
    """Predicate that always evaluates to False.

    Parameters
    ----------
    args: any
        Variable list of arguments.

    Returns
    -------
    bool
    """
    return False


class eval_all(object):
    """Logic operator that evaluates a list of predicates and returns True only
    if all predicates return a defined result value.
    """
    def __init__(self, predicates, truth_value=True):
        """Initialize the list of predicates and the expected result value.

        Parameters
        ----------
        predicates: list(callable)
            List of callables that are evaluated on a given value.
        truth_value: scalar, default=True
            Expected result value for predicate evaluation to be considered
            satisfied.
        """
        self.predicates = predicates
        self.truth_value = truth_value

    def __call__(self, value):
        """Evaluate all predicates on the given value. Returns True only if all
        predicates evaluate to the defined result value.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        for f in self.predicates:
            if f.eval(value) != self.truth_value:
                return False
        return True


def is_list_or_tuple(value):
    """Test if a given value is a list or tuple that can be converted into
    multiple arguments.

    Parameters
    ----------
    value: any
        Any object that is tested for being a list or tuple.

    Returns
    -------
    bool
    """
    return isinstance(value, list) or isinstance(value, tuple)


def scalar_pass_through(value):
    """Pass-through method for single scalar values.

    Parameters
    ----------
    value: scalar
        Scalar cell value from a data frame row.

    Returns
    -------
    scalar
    """
    return value


def tenary_pass_through(*args):
    """Pass-through method for a list of argument values.

    Parameters
    ----------
    args: list of scalar
        List of argument values.

    Returns
    -------
    scalar
    """
    return args


def unique_identifier(length: Optional[int] = None) -> str:
    """Get an identifier string of given length. Uses UUID to generate a unique
    string and return the requested number of characters from that string.

    Parameters
    ----------
    length: int, default=None
        Number of characters in the returned string.

    Returns
    -------
    string
    """
    identifier = str(uuid.uuid4()).replace('-', '')
    if length is not None:
        identifier = identifier[:length]
    return identifier
