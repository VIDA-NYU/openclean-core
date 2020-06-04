# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions and classes that implement the split operator in openclean."""

from openclean.function.eval.base import EvalFunction
from openclean.operator.base import DataFrameSplitter
from openclean.operator.transform.filter import get_predicate


# -- Functions ----------------------------------------------------------------

def split(df, columns=None, predicate=None):
    """Split function for data frames. Evaluates a Boolean predicate on the
    rows of a given data frame. The output comprises two data frames. The
    first data frame contains the rows for which the predicate was satisfied
    and the second contains the rows for which the predicate was not satisfied.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.
    predicate: (
            openclean.function.eval.base.EvalFunction,
            openclean.function.eval.base.value.ValueFunction,
            callable,
            dictionary,
            or scalar
        )
        Evaluation function or callable that accepts a data frame row as the
        only argument (if columns is None). ValueFunction or callable if one
        or more columns are specified. If columns are given the function also
        accepts a dictionary or scalar value as argument. These will be wrapped
        accordingly.

    Returns
    -------
    pandas.DataFrame, pandas.DataFrame
    """
    predicate = get_predicate(columns=columns, predicate=predicate)
    return Split(predicate=predicate).split(df)


# -- Operators ----------------------------------------------------------------

class Split(DataFrameSplitter):
    """Data frame splitter that evaluates a Boolean predicate on the rows of
    a data frame. The output has two data frames, one containing the rows for
    which the predicate was satisfied and one containing the rows for which the
    predicate was not satisfied.
    """
    def __init__(self, predicate):
        """Initialize the predicate that is evaluated.

        Parameters
        ----------
        predicate: callable
            Callable that accepts a data frame row as the only argument.
        """
        self.predicate = predicate

    def split(self, df):
        """Split the data frame into two data frames. The output is a tuple.
        The first element is the data frame that contains all rows for which
        the predicate evaluated to True. The second element is a data frame
        containing the rows for which the predicate was False.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame, pandas.DataFrame
        """
        # Prepare the predicate if it is an evaluation function.
        if isinstance(self.predicate, EvalFunction):
            self.predicate.prepare(df)
        # Create a Boolean array to maintain information about those rows that
        # satisfy the predicate (true map) and those that do not satisfy the
        # predicate (false map).
        tmap = [False] * len(df.index)
        fmap = [False] * len(df.index)
        index = 0
        for rowid, values in df.iterrows():
            if self.predicate.eval(values):
                tmap[index] = True
            else:
                fmap[index] = True
            index += 1
        # Return one data frame containing only those rows for which the
        # predicate was satisfied and one data frame containing those rows
        # for which the predicate was not satisfied.
        return df[tmap], df[fmap]
