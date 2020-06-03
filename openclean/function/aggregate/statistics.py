# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of evaluation functions that return a computed statistic over
one or more data frame columns for all data frame rows.
"""

from openclean.data.column import select_clause
from openclean.function.base import EvalFunction


# -- Generic prepared statistics function -------------------------------------

class ColumnStats(EvalFunction):
    def __init__(self, func, columns=None):
        """Initialize the statistics function and the list of columns on which
        the function will be applied.

        Parameters
        ----------
        func: callable
            Function that accepts values from one or more columns as input.
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        """
        self.func = func
        self.columns = columns
        # The statistics value will be initialized by the prepare() method.
        self.value = None

    def exec(self, values):
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
        return self.value

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
        openclean.function.base.EvalFunction
        """
        # Determine the list of column indices over which the statistics will
        # be computed
        if self.columns is None:
            colidxs = range(len(df.columns))
        elif isinstance(self.columns, list):
            _, colidxs = select_clause(df, columns=self.columns)
        else:
            _, colidxs = select_clause(df, columns=[self.columns])
        # If we have more than one column create a list of values that
        # concatenates the values from the individual columns.
        if len(colidxs) > 1:
            values = list()
            for c in colidxs:
                values.extend(list(df.iloc[:, c]))
            self.value = self.func(values)
        else:
            self.value = self.func(list(df.iloc[:, colidxs[0]]))
        return self


# -- Shortcuts for common statistics methods ----------------------------------

class Max(ColumnStats):
    """Evaluation function that returns the maximum of values for one or more
    columns in a data frame as the result value for all columns in the data
    frame.
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


class Mean(ColumnStats):
    """Evaluation function that returns the mean of values for one or more
    columns in a data frame as the result value for all columns in the data
    frame.
    """
    def __init__(self, columns=None):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        """
        import statistics
        super(Mean, self).__init__(func=statistics.mean, columns=columns)


class Min(ColumnStats):
    """Evaluation function that returns the minimum of values for one or more
    columns in a data frame as the result value for all columns in the data
    frame.
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
