# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test for containment of column values in value sets."""

import pandas as pd

from openclean.data.util import to_set
from openclean.function.eval.base import Eval
from openclean.function.value.domain import IsInDomain, IsNotInDomain


class IsIn(Eval):
    """Boolean predicate to tests whether a value (or list of values) belong(s)
    to a domain of known values.
    """
    def __init__(self, columns, domain):
        """Create an instance of an evaluation function that checks for domain
        inclusion.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        """
        # Convert pandas data frames or series into a set of values.
        if type(domain) in [pd.DataFrame, pd.Series]:
            domain = to_set(domain)
        super(IsIn, self).__init__(
            func=IsInDomain(domain),
            columns=columns,
            is_unary=True
        )


class IsNotIn(Eval):
    """Boolean predicate that tests whether a value (or list of values) dos not
    belong to a domain of knwon values.
    """
    def __init__(self, columns, domain):
        """Create an instance of an evaluation function that checks for domain
        exclusion.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        domain: pandas.DataFrame, pandas.Series, or object
            Data frame or series, or any object that implements the
            __contains__ method.
        """
        # Convert pandas data frames or series into a set of values.
        if type(domain) in [pd.DataFrame, pd.Series]:
            domain = to_set(domain)
        super(IsNotIn, self).__init__(
            func=IsNotInDomain(domain),
            columns=columns,
            is_unary=True
        )
