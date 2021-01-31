# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of scalar predicates that test domain membership."""

import pandas as pd

from openclean.data.util import to_set
from openclean.function.matching.base import StringMatcher
from openclean.function.value.base import PreparedFunction

import openclean.util.core as util


class BestMatch(PreparedFunction):
    """Value function that returns for a given value the best matching similar
    value in a controlled vocabulary.
    """
    def __init__(self, matcher: StringMatcher):
        """Initialize the matcher for the associated controlled vocabulary.

        Parameters
        ----------
        matcher: openclean.function.matching.base.StringMatcher
            Matcher that computes bast matches for the terms in a controlled
            vocabulary.
        """
        self.matcher = matcher

    def eval(self, value):
        """Return the base matching value for a given query in the associated
        vocabulary. If the query term is in the vocabulary it is returned as
        the result. If the term is not in the vocabulary the best matching
        values (using the associated vocabulary matcher) are found. If no
        bast match is found or if multiple best matches are found a ValueError
        is raised. Otherwise, the best matching value is returned as the
        function result.

        Parameters
        ----------
        value: scalar
            Scalar value for which the best matching value in the associated
            vocabulary is computed.

        Returns
        -------
        bool

        Raises
        ------
        ValueError
        """
        if value in self.matcher.vocabulary:
            return value
        matches = self.matcher.matched_values(value)
        if len(matches) == 0:
            raise ValueError("no match for '{}'".format(value))
        elif len(matches) > 1:
            raise ValueError("multiple matches for '{}'".format(value))
        return matches[0]


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
        # Convert all string values in the domain to lower cases.
        self.domain = [to_lower(v) for v in domain] if ignore_case else domain

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
        if self.ignore_case:
            value = to_lower(value)
        is_in = value in self.domain
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


# -- Helper funcitons ---------------------------------------------------------

def to_lower(value):
    """Convert a given value to lower case. Handles the case where the value is
    a list or tuple.

    Parameters
    ----------
    value: string, list, or tuple
        Value that is transformed to lower case.

    Returns
    -------
    string, list, or tuple
    """
    if util.is_list_or_tuple(value):
        return tuple([v.lower() for v in value])
    elif isinstance(value, str):
        return value.lower()
    return value
