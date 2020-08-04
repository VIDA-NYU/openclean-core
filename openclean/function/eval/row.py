# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of evaluation functions that compute a result over a list of
values that are extracted for data frame rows.
"""

from openclean.function.eval.base import EvalFunction, to_const_eval


# -- Generic function for lists of values -------------------------------------

class RowAggregator(EvalFunction):
    """The row aggregator evaluates a given function (callable) on a list of
    values extracted from different cells in the same data frame row using
    evaluation functions.

    This function allows, for example, to compute the minimum or maximum over
    values from different columns in a data frame row.
    """
    def __init__(self, aggregator, values):
        """Initialize the callable and the list of evaluation functions that
        are used to extract values that are used as arguments for the list
        function.

        Parameters
        ----------
        aggregator: callable
            Function that accepts a list of values as input.
        values: list(openclean.function.eval.base.EvalFunction)
            List of evaluation functions that are used to generate the inputs
            for the list function (e.g., extract values from columns in a data
            frame row).
        """
        self.aggregator = aggregator
        self.values = values if isinstance(values, list) else [values]

    def eval(self, values):
        """Evaluate the value functions on the data frame row. The collected
        results are then passed to the list function to compute the return
        value for this function.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar
        """
        return self.aggregator([f.eval(values) for f in self.values])

    def prepare(self, df):
        """Prepare the associated evaluation functions.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        return RowAggregator(
            aggregator=self.aggregator,
            values=[f.prepare(df) for f in self.values]
        )


# -- Shortcuts for common list functions --------------------------------------

class Greatest(RowAggregator):
    """Evaluation function that returns the maximum value for a list of values
    from different cells in a data frame row.
    """
    def __init__(self, *args):
        """Initialize the statistics function in the super class as well as the
        list of values (e.g., columns) on which the function will be applied.

        Parameters
        ----------
        args: list(string, or openclean.function.eval.base.EvalFunction)
            List of evaluation functions that are used to generate the inputs
            for the list function (e.g., extract values from columns in a data
            frame row).
        """
        values = [to_const_eval(arg) for arg in args]
        super(Greatest, self).__init__(aggregator=max, values=values)


class Least(RowAggregator):
    """Evaluation function that returns the minimum of values for one or more
    columns in a data frame as the result value for all columns in the data
    frame.
    """
    def __init__(self, *args):
        """Initialize the statistics function in the super class as well as the
        list of values (e.g., columns) on which the function will be applied.

        Parameters
        ----------
        args: list(string, or openclean.function.eval.base.EvalFunction)
            List of evaluation functions that are used to generate the inputs
            for the list function (e.g., extract values from columns in a data
            frame row).
        """
        values = [to_const_eval(arg) for arg in args]
        super(Least, self).__init__(aggregator=min, values=values)
