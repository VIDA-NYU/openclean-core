# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions and classes that implement the filter operators in openclean."""

from openclean.function.base import Eval, EvalFunction
from openclean.function.column import Col
from openclean.function.constant import Const
from openclean.function.predicate.comp import Eq
from openclean.function.predicate.domain import IsIn
from openclean.function.value.base import CallableWrapper, ValueFunction
from openclean.operator.base import DataFrameTransformer


# -- Functions ----------------------------------------------------------------

def delete(df, columns=None, predicate=None):
    """Delete row function for data frames. Returns a data frame that contains
    the rows of the input data frame for which the given predicate(s) evaluates
    to False. This is the negation of the filter function.

    Like the filter function, this function operates in two modes. If a list of
    columns is given thr predicates are expected to be ValueFunctions. If no
    columns are specified the predicates are expected to be a single
    EvalFunction that operate on data frame rows.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    predicate: (
            openclean.function.base.EvalFunction,
            openclean.function.base.value.ValueFunction,
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
    pandas.DataFrame
    """
    return filter(df=df, columns=columns, predicate=predicate, negated=True)


def filter(df, columns=None, predicate=None, negated=False):
    """Filter function for data frames. Returns a data frame that only contains
    the rows of the input data frame for which the given predicate(s) evaluates
    to True.

    This function operates in two modes. If a list of columns is given the
    predicates are expected to be ValueFunctions. If no columns are specified
    the predicates are expected to be a single EvalFunction that operate on
    data frame rows.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    predicate: (
            openclean.function.base.EvalFunction,
            openclean.function.base.value.ValueFunction,
            callable,
            dictionary,
            or scalar
        )
        Evaluation function or callable that accepts a data frame row as the
        only argument (if columns is None). ValueFunction or callable if one
        or more columns are specified. If columns are given the function also
        accepts a dictionary or scalar value as argument. These will be wrapped
        accordingly.
    negated: bool, default=False
        Negate the predicate value to get an inverted result.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    # If columns is a callable or eval function and predicate is None we
    # flip the columns and predicate values.
    if predicate is None and columns is not None:
        if isinstance(columns, EvalFunction) or callable(predicate):
            predicate = columns
            columns = None
    # Raise a ValueError if no predicate is given.
    if predicate is None:
        raise ValueError('missing predicate')
    # If one or more columns are specified, the predicate is expected to be
    # a ValueFunction. If it is a callable the function will be wrapped. A
    # dictionary will be converted to a dommain lookup. All other values are
    # treated as constants.
    if columns is not None:
        # Ensure that columns is a list.
        if not isinstance(columns, list):
            columns = [columns]
        # Convert predicate to an evaluatin function.
        if isinstance(predicate, ValueFunction):
            predicate = Eval(func=predicate, columns=columns)
        elif callable(predicate):
            predicate = Eval(func=CallableWrapper(predicate), columns=columns)
        elif isinstance(predicate, dict):
            predicate = IsIn(columns=columns, domain=predicate)
        else:
            predicate = Eq(Col(columns), Const(predicate))
    elif not isinstance(predicate, EvalFunction):
        predicate = Eval(func=predicate, columns=columns)
    return Filter(predicate=predicate, negated=negated).transform(df)


# -- Operators ----------------------------------------------------------------

class Filter(DataFrameTransformer):
    """Data frame transformer that evaluates a Boolean predicate on the rows of
    a data frame. The transformed output contains only those rows for which the
    predicate evaluated to True.
    """
    def __init__(self, predicate, negated=False):
        """Initialize the predicate that is evaluated.

        Parameters
        ----------
        predicate: openclean.function.base.EvalFunction or callable
            Evaluation function or callable that accepts a data frame row as
            the only argument.
        negated: bool, default=False
            Negate the predicate value to get an inverted result.
        """
        self.predicate = predicate
        self.negated = negated

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
        # Negate the satisfy array if the negated flag is True.
        if self.negated:
            smap = [not v for v in smap]
        # Return data frame containing only those rows for which the predicate
        # was satisfied.
        return df[smap]
