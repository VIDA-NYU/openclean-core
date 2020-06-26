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

from openclean.data.column import select_clause
from openclean.function.eval.base import EvalFunction

import openclean.util as util


# -- Generic prepared statistics function -------------------------------------

class ColumnAggregator(EvalFunction):
    """Evaluation functin that computes a statistic value over one or more
    columns in a data frame and returns this value as result for all calls to
    the eval() method.
    """
    def __init__(self, func, columns=None, value=None):
        """Initialize the statistics function and the list of columns on which
        the function will be applied.

        Raises a ValueError if the given function is not callable.

        Parameters
        ----------
        func: callable
            Function that accepts values from one or more columns as input.
        columns: int, string, or list(int or string), default=None
            Single column or list of column index positions or column names.
        value: int, float, or tuple, default=None
            Constant value that will be returned by the eval() method. This
            value is only set if the function has been prepared.
        """
        # This will raise a ValueError if the function is not callable.
        self.func = util.ensure_callable(func)
        self.columns = columns
        self._value = value

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
        return self._value

    def is_prepared(self):
        """The function needs preparation if the constant return value is None.

        Returns
        -------
        bool
        """
        return self._value is not None

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
        # Determine the list of column indices over which the statistics will
        # be computed
        if self.columns is None:
            colidx = range(len(df.columns))
        elif isinstance(self.columns, list):
            _, colidx = select_clause(df, columns=self.columns)
        else:
            _, colidx = select_clause(df, columns=[self.columns])
        # If we have more than one column create a list of values that
        # concatenates the values from the individual columns.
        if len(colidx) > 1:
            values = list()
            for c in colidx:
                values.extend(list(df.iloc[:, c]))
            value = self.func(values)
        else:
            value = self.func(list(df.iloc[:, colidx[0]]))
        return ColumnAggregator(
            func=self.func,
            columns=self.columns,
            value=value
        )


# -- Shortcuts for common statistics methods ----------------------------------

class Avg(ColumnAggregator):
    """Evaluation function that returns the mean of values for one or more
    columns in a data frame.
    """
    def __init__(self, columns=None):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        """
        super(Avg, self).__init__(func=np.mean, columns=columns)


class Max(ColumnAggregator):
    """Evaluation function that returns the maximum of values for one or more
    columns in a data frame.
    """
    def __init__(self, columns=None):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        """
        super(Max, self).__init__(func=max, columns=columns)


class Min(ColumnAggregator):
    """Evaluation function that returns the minimum of values for one or more
    columns in a data frame.
    """
    def __init__(self, columns=None):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        """
        super(Min, self).__init__(func=min, columns=columns)


class Sum(ColumnAggregator):
    """Evaluation function that returns the sum over values for one or more
    columns in a data frame.
    """
    def __init__(self, columns=None):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        """
        super(Sum, self).__init__(func=sum, columns=columns)
