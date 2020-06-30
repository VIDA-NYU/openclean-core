# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of evaluation functions that return a computed statistic over
one or more data frame columns for all data frame rows.
"""

import numpy as np

from openclean.function.eval.base import (
    Col, Const, EvalFunction, to_column_eval
)

import openclean.util as util


# -- Generic prepared statistics function -------------------------------------

class ColumnAggregator(EvalFunction):
    """Evaluation function that computes a statistic value over one or more
    columns in a data frame and returns this value as result for all calls to
    the eval() method.
    """
    def __init__(self, func, columns):
        """Initialize the statistics function and the list of columns on which
        the function will be applied.

        Raises a ValueError if the given function is not callable.

        Parameters
        ----------
        func: callable
            Function that accepts values from one or more columns as input.
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        value: int, float, or tuple, default=None
            Constant value that will be returned by the eval() method. This
            value is only set if the function has been prepared.
        """
        # This will raise a ValueError if the function is not callable.
        self.func = util.ensure_callable(func)
        # Ensure that columns contains only evaluation functions.
        if isinstance(columns, tuple):
            columns = tuple([to_column_eval(c) for c in columns])
        elif isinstance(columns, list):
            columns = [to_column_eval(c) for c in columns]
        elif not isinstance(columns, EvalFunction):
            columns = Col(columns)
        self.columns = columns

    def eval(self, values):
        """The execute method for the evaluation functon returns the computed
        statistic value for each row in the data frame.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar
        """
        raise RuntimeError('column aggregator not prepared')

    def prepare(self, df):
        """Prepare the evaluation function by computing the statistics value
        that is returned for each data frame row. Applies the function that
        was given at object instantiation on the specified list of columns.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        values = list()
        if isinstance(self.columns, tuple):
            producer = tuple([f.prepare(df) for f in self.columns])
            for _, row in df.iterrows():
                values.append(tuple([f.eval(row) for f in producer]))
        elif isinstance(self.columns, list):
            producer = [f.prepare(df) for f in self.columns]
            for _, row in df.iterrows():
                values.append([f.eval(row) for f in producer])
        else:
            producer = self.columns.prepare(df)
            for _, row in df.iterrows():
                values.append(producer.eval(row))
        return Const(self.func(values))


# -- Shortcuts for common statistics methods ----------------------------------

class Avg(ColumnAggregator):
    """Evaluation function that returns the mean of values for one or more
    columns in a data frame.
    """
    def __init__(self, columns):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        """
        super(Avg, self).__init__(func=np.mean, columns=columns)


class Count(ColumnAggregator):
    """Evaluation function that counts the number of values in one or more
    columns that match a given value.
    """
    def __init__(self, columns, value=True):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        value: any, default=True
            Value whose frequency is counted.
        """

        def count(values):
            counter = 0
            for val in values:
                if val == value:
                    counter += 1
            return counter

        super(Count, self).__init__(func=count, columns=columns)


class Max(ColumnAggregator):
    """Evaluation function that returns the maximum of values for one or more
    columns in a data frame.
    """
    def __init__(self, columns):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        """
        super(Max, self).__init__(func=max, columns=columns)


class Min(ColumnAggregator):
    """Evaluation function that returns the minimum of values for one or more
    columns in a data frame.
    """
    def __init__(self, columns):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        """
        super(Min, self).__init__(func=min, columns=columns)


class Sum(ColumnAggregator):
    """Evaluation function that returns the sum over values for one or more
    columns in a data frame.
    """
    def __init__(self, columns):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        """
        super(Sum, self).__init__(func=sum, columns=columns)
