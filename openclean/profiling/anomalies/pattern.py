# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Outlier detection algorithms using regular expressions. Pattern outliers in
general are considered values that do not match a (list of) pattern(s) that
the values in a list (e.g., data frame column) are expected to satisfy.
"""

from typing import List, Optional, Union

import pandas as pd

from openclean.data.types import Value
from openclean.function.eval.base import InputColumn
from openclean.function.value.regex import IsMatch, IsNotMatch
from openclean.profiling.anomalies.conditional import ConditionalOutliers
from openclean.util.core import always_false, eval_all


def regex_outliers(
    df: pd.DataFrame, columns: Union[InputColumn, List[InputColumn]],
    patterns: List[str], fullmatch: Optional[bool] = True
) -> List:
    """Identify values in a (list of) data frame columns(s) that do not match
    any of the given pattern expressions. Patterns are represented as strings
    in the Python Regular Expression Syntax.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: list, tuple, or openclean.function.eval.base.EvalFunction
        Evaluation function to extract values from data frame rows. This
        can also be a list or tuple of evaluation functions or a list of
        column names or index positions.
    patterns: list(string)
        List if regular expression patterns.
    fullmatch: bool, default=True
        If True, the pattern has to match a given string fully in order to
        not be considered an outlier.

    Returns
    -------
    list
    """
    op = RegExOutliers(patterns=patterns, fullmatch=fullmatch)
    return op.run(df=df, columns=columns)


class RegExOutliers(ConditionalOutliers):
    """Identify values in a (list of) data frame columns(s) that do not match
    any of the given pattern expressions. Patterns are represented as strings
    in the Python Regular Expression Syntax.
    """
    def __init__(self, patterns: List[str], fullmatch: Optional[bool] = True):
        """Initialize the list of patterns and the matching condition.

        patterns: list(string)
            List if regular expression patterns.
        fullmatch: bool, default=True
            If True, the pattern has to match a given string fully in order to
            not be considered an outlier.
        """
        super(RegExOutliers, self).__init__()
        # Ensure that patterns is a list.
        if not isinstance(patterns, list):
            patterns = [patterns]
        # Set the predicate that is used to identify outliers. Distinguish
        # based on number of elements in the pattern list.
        if len(patterns) == 0:
            # An empty pattern list means that no value is being considered as
            # an outlier.
            self.predicate = always_false
        elif len(patterns) == 1:
            self.predicate = IsNotMatch(
                pattern=patterns[0],
                fullmatch=fullmatch
            )
        else:
            # If a list of patterns is given a value i
            ops = [IsMatch(pattern=p, fullmatch=fullmatch) for p in patterns]
            self.predicate = eval_all(predicates=ops, truth_value=False)

    def outlier(self, value: Value) -> bool:
        """Test if a given value is a match for the associated regular
        expressions. If the value is not a match it is considered an outlier.

        Returns a dictionary for values that are classified as outliers that
        contains one element 'value' for the tested value.

        Parameters
        ----------
        value: scalar or tuple
            Value that is being tested for the outlier condition.

        Returns
        -------
        bool
        """
        if self.predicate(value):
            return value
