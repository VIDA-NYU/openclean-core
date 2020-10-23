# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions and classes that implement the filter operators in openclean."""

from typing import Optional

import pandas as pd

from openclean.function.eval.base import EvalFunction
from openclean.operator.base import DataFrameTransformer


# -- Functions ----------------------------------------------------------------

def delete(df: pd.DataFrame, predicate: EvalFunction) -> pd.DataFrame:
    """Delete rows in a data frame. The delete operator evaluates a given
    predicate on all rows in a data frame. It returns a new data frame where
    those rows that satisfied the predicate are deleted.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame.
    predicate: openclean.function.eval.base.EvalFunction
        Evaluation function that is expected to return a Boolean value when
        evaluated on a data frame row. All rows in the input data frame that
        satisfy the predicate will be deleted.

    Returns
    -------
    pd.DataFrame
    """
    return filter(df=df, predicate=predicate, negated=True)


def filter(
    df: pd.DataFrame, predicate: EvalFunction, negated: Optional[bool] = False
) -> pd.DataFrame:
    """Filter function for data frames. Returns a data frame that only contains
    the rows of the input data frame for which the given predicate evaluates
    to True.

    Parameters
    ----------
    df: pd.DataFrame
        Input data frame.
    predicate: openclean.function.eval.base.EvalFunction
        Evaluation function that is expected to return a Boolean value when
        evaluated on a data frame row. Only those rows in the input data frame
        that satisfy the predicate will be included in the result.
    negated: bool, default=False
        Negate the predicate value to get an inverted result.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return Filter(predicate=predicate, negated=negated).transform(df)


# -- Operators ----------------------------------------------------------------

class Filter(DataFrameTransformer):
    """Data frame transformer that evaluates a Boolean predicate on the rows of
    a data frame. The transformed output contains only those rows for which the
    predicate evaluated to True (or Flase if the negated flag is True).
    """
    def __init__(self, predicate: EvalFunction, negated: Optional[bool] = False):
        """Initialize the predicate that is evaluated.

        Parameters
        ----------
        predicate: openclean.function.eval.base.EvalFunction
            Evaluation function that is evaluated on a given data frame.
        negated: bool, default=False
            Negate the predicate value to get an inverted result.
        """
        self.predicate = predicate
        self.negated = negated

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a data frame that contains only those rows from the given
        input data frame that satisfy the filter condition.

        Parameters
        ----------
        df: pd.DataFrame
            Input data frame.

        Returns
        -------
        pd.DataFrame
        """
        smap = self.predicate.eval(df)
        if self.negated:
            smap = [not v for v in smap]
        return df[smap]
