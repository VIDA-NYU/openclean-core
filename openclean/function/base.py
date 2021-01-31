# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of basic helper functions and classes."""

# -- Quantifier functions -----------------------------------------------------

"""Helper classes to evaluate single-value functions on lists of arguments."""


class All(object):
    """Predicate that tests if all values in a given list evaluate to True for
    a given predicate.
    """
    def __init__(self, predicate):
        """Initialize the evaluated predicate.

        Parameters
        ----------
        predicate: callable
            Predicate that is evaluated on argument values.
        """
        self.predicate = predicate

    def __call__(self, *args):
        """Returns True if all given argument values evaluate to True.

        Parameters
        ----------
        *args: list
            List of (scalar) values from a data frame row.

        Returns
        -------
        bool
        """
        for val in args:
            if not self.predicate(val):
                return False
        return True


class One(object):
    """Predicate that tests if at least one value in a given list evaluates to
    True for a given predicate.
    """
    def __init__(self, predicate):
        """Initialize the evaluated predicate.

        Parameters
        ----------
        predicate: callable
            Predicate that is evaluated on argument values.
        """
        self.predicate = predicate

    def __call__(self, *args):
        """Returns True if at least one of the given argument values is empty.

        Parameters
        ----------
        *args: list
            List of (scalar) values from a data frame row.

        Returns
        -------
        bool
        """
        for val in args:
            if self.predicate(val):
                return True
        return False
