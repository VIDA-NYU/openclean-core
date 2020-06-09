# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test for containment of column values in value sets."""

import pandas as pd

from openclean.data.util import to_set
from openclean.function.eval.base import Eval, is_var_func
from openclean.function.value.domain import IsInDomain, IsNotInDomain


# -- Callable classes for containment checks for lists of values --------------

class ContainsTuple(object):
    """Callable that wraps a list of values for containment checking."""
    def __init__(self, domain):
        """Initialize the domain of valid (or known) values.

        Parameters
        ----------
        domain: object
            Object that implements the __contains__ method
        """
        self.domain = domain

    def __call__(self, *args):
        """Make the object callable for use in data frame row predicates.

        Parameters
        ----------
        *args: list
            List of (scalar) values from a data frame row.

        Returns
        -------
        bool
        """
        return args in self.domain


class NotContainsTuple(object):
    """Callable that wraps a list of values that defines the domain of valid
    values. The dictionary is used to identify those values that don'o't belong
    to the domain.
    """
    def __init__(self, domain):
        """Initialize the domain of valid (or known) values.

        Parameters
        ----------
        domain: object
            Object that implements the __contains__ method
        """
        self.domain = domain

    def __call__(self, *args):
        """Make the object callable for use in data frame row predicates.

        Parameters
        ----------
        *args: list
            List of (scalar) values from a data frame row.

        Returns
        -------
        bool
        """
        return args not in self.domain


# -- Predicate factory patterns -----------------------------------------------

class IsIn(object):
    """Boolean predicate to tests whether a value (or list of values) belong(s)
    to a domain of known values.
    """
    def __new__(cls, columns, domain):
        """Create an instance of an evaluation function that checks for domain
        inclusion.

        Parameters
        ----------
        columns: string or int or list
            Name or index of the column(s) on which the predicate is evaluated.
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        """
        # Convert pandas data frames or series into a set of values.
        if type(domain) in [pd.DataFrame, pd.Series]:
            domain = to_set(domain)
        if is_var_func(columns):
            predicate = ContainsTuple(domain)
        else:
            predicate = IsInDomain(domain)
        return Eval(func=predicate, columns=columns)


class IsNotIn(object):
    """Boolean predicate that tests whether a value (or list of values) dos not
    belong to a domain of knwon values.
    """
    def __new__(cls, columns, domain):
        """Create an instance of an evaluation function that checks for domain
        exclusion.

        Parameters
        ----------
        columns: string or int or list
            Name or index of the column(s) on which the predicate is evaluated
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        """
        # Convert pandas data frames or series into a set of values.
        if type(domain) in [pd.DataFrame, pd.Series]:
            domain = to_set(domain)
        if is_var_func(columns):
            predicate = NotContainsTuple(domain)
        else:
            predicate = IsNotInDomain(domain)
        return Eval(func=predicate, columns=columns)
