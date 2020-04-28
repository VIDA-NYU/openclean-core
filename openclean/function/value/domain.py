# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of scalar predicates that test domain membership."""

import pandas as pd

from openclean.data.transform import to_set
from openclean.function.value.base import scalar_pass_through
from openclean.function.value.string import lower


class is_in(object):
    """Callable that wrapps a list of values for containment checking."""
    def __init__(self, domain, ignore_case=False):
        """Initialize the domain of valid (or known) values.

        Parameters
        ----------
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        ignore_case: bool, optional
            Ignore case for domain inclusion checking
        """
        # Convert pandas data frames or series into a set of values.
        if type(domain) in [pd.DataFrame, pd.Series]:
            domain = to_set(domain)
        self.ignore_case = ignore_case
        if ignore_case:
            self.valfunc = lower
            self.domain = [self.valfunc(v) for v in domain]
        else:
            self.valfunc = scalar_pass_through
            self.domain = domain

    def __call__(self, value):
        """Test if a given value is a member of the domain of known values.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being a domain member.

        Returns
        -------
        bool
        """
        return self.valfunc(value) in self.domain


class is_not_in(object):
    """Callable that wrapps a list of values that define the domain of valid
    values. The list is used to identify those values that do not belong to the
    domain.
    """
    def __init__(self, domain, ignore_case=False):
        """Initialize the domain of valid (or known) values.

        Parameters
        ----------
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        ignore_case: bool, optional
            Ignore case for domain inclusion checking
        """
        # Convert pandas data frames or series into a set of values.
        if type(domain) in [pd.DataFrame, pd.Series]:
            domain = to_set(domain)
        self.ignore_case = ignore_case
        if ignore_case:
            self.valfunc = lower
            self.domain = [self.valfunc(v) for v in domain]
        else:
            self.valfunc = scalar_pass_through
            self.domain = domain

    def __call__(self, value):
        """Test if a given value is contained in the domain of known values.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for not being a domain member.

        Returns
        -------
        bool
        """
        return self.valfunc(value) not in self.domain
