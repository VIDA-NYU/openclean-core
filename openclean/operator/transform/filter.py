# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions and classes that implement the filter operators in openclean."""

from openclean.function.base import EvalFunction
from openclean.operator.base import DataFrameTransformer


# -- Functions ----------------------------------------------------------------

def filter(df, columns=None, predicates=None, truth_value=True):
    """Filter function for data frames. Returns a data frame that only contains
    the rows of the input data frame for which the given predicate(s) evaluates
    to True.

    This function operates in two modes. If a list of columns is given the
    predicates are expected to be ValueFunctions. If no columns are specified
    the predicates are expected to be a single EvalFunction that operate ...
    
    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    predicate: openclean.function.base.EvalFunction or callable
        Evaluation function or callable that accepts a data frame row as the
        only argument.

    Returns
    -------
    pandas.DataFrame
    """
    return Filter(predicate=predicate).transform(df)


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
        predicate: openclean.function.base.EvalFunction or callable
            Evaluation function or callable that accepts a data frame row as
            the only argument.
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
