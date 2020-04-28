# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions and classes that implement the filter operator in openclean."""

from openclean.function.base import EvalFunction
from openclean.operator.base import DataFrameTransformer


# -- Functions ----------------------------------------------------------------

def filter(df, predicate):
    """Filter function for data frames. Returns a data frame that only contains
    the rows of the input data frame for which the given predicate evaluates
    to True.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    predicate: callable
        Callable that accepts a data frame row as the only argument.

    Returns
    -------
    pandas.DataFrame
    """
    return Filter(predicate=predicate).transform(df)


def ignore(df, predicate=None):
    """Filter function for data frames that is used to remove rows that satisfy
    a given condition. Returns a data frame that only contains the rows of the
    input data frame for which the given predicate evaluates to False. If not
    predicate is given the result is the input data frame.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    predicate: callable, optional
        Callable that accepts a data frame row as the only argument.

    Returns
    -------
    pandas.DataFrame
    """
    if predicate is not None:
        return Ignore(predicate=predicate).transform(df)
    else:
        return df


# -- Operators ----------------------------------------------------------------

class Filter(DataFrameTransformer):
    """Data frame transformer that evaluates a Boolean predicate on the rows of
    a data frame. The transformed output contains only those rows for which the
    predicate evaluated to True.
    """
    def __init__(self, predicate):
        """Initialize the predicate that is evaluated.

        Parameters
        ----------
        predicate: callable
            Callable that accepts a data frame row as the only argument.
        """
        self.predicate = predicate

    def transform(self, df):
        """Return a data frame that contains only those rows from the given
        input data frame that satisfy the filter condition.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        # Prepare the predicate if it is an evaluation function.
        if isinstance(self.predicate, EvalFunction):
            self.predicate.prepare(df)
        # Create a Boolean array to maintain information about the rows that
        # satisfy the predicate.
        smap = [False] * len(df.index)
        index = 0
        for rowid, values in df.iterrows():
            smap[index] = self.predicate(values)
            index += 1
        # Return data frame containing only those rows for which the predicate
        # was satisfied.
        return df[smap]


class Ignore(Filter):
    """Data frame transformer that evaluates a Boolean predicate on the rows of
    a data frame. The transformed output contains only those rows for which the
    predicate evaluated to False. That is, rows that satisfy the predicate are
    removed.
    """
    def __init__(self, predicate):
        """Initialize the predicate that is evaluated.

        Parameters
        ----------
        predicate: callable
            Callable that accepts a data frame row as the only argument.
        """
        self.predicate = predicate

    def transform(self, df):
        """Return a data frame that contains those rows from the given input
        data frame that do not satisfy the filter condition. Rows that satisfy
        the condition are ignored.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        # Prepare the predicate if it is an evaluation function.
        if isinstance(self.predicate, EvalFunction):
            self.predicate.prepare(df)
        # Create a Boolean array to maintain information about the rows that
        # do not satisfy the predicate.
        smap = [True] * len(df.index)
        index = 0
        for rowid, values in df.iterrows():
            smap[index] = not self.predicate(values)
            index += 1
        # Return data frame containing only those rows for which the predicate
        # was not satisfied.
        return df[smap]
