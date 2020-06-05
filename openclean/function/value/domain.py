# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of scalar predicates that test domain membership."""

import pandas as pd

from openclean.data.transform import to_set
from openclean.function.base import PreparedFunction, scalar_pass_through
from openclean.function.value.string import Lower


class IsInDomain(PreparedFunction):
    """Callable function that wrapps a list of values for containment checking.
    If the ignore case flag is True, all string values in the domain are
    converted to lower case.
    """
    def __init__(self, domain, ignore_case=False, negated=False):
        """Initialize the domain of valid (or known) values.

        Parameters
        ----------
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        ignore_case: bool, default=False
            Ignore case for domain inclusion checking
        negated: bool, default=False
            Negate the return value of the function to check for values that
            are not included in the domain.
        """
        # Convert pandas data frames or series into a set of values.
        if type(domain) in [pd.DataFrame, pd.Series]:
            domain = to_set(domain)
        self.negated = negated
        self.ignore_case = ignore_case
        if ignore_case:
            # Convert all string values in the domain to lower cases.
            self.valfunc = Lower()
            self.domain = [self.valfunc(v) for v in domain]
        else:
            self.valfunc = scalar_pass_through
            self.domain = domain

    def eval(self, value):
        """Test if a given value is a member of the domain of known values.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being a domain member.

        Returns
        -------
        bool
        """
        is_in = self.valfunc(value) in self.domain
        return is_in != self.negated


class IsNotInDomain(IsInDomain):
    """Callable that wrapps a list of values that define the domain of valid
    values. The list is used to identify those values that do not belong to the
    domain. If the ignore case flag is True, all string values in the domain
    are converted to lower case.
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
        super(IsNotInDomain, self).__init__(
            domain=domain,
            ignore_case=ignore_case,
            negated=True
        )
