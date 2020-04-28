# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Evaluation functions that return a constant value."""

from openclean.function.base import EvalFunction


class Const(EvalFunction):
    """Evaluation function that returns a constant value for each data frame
    row.
    """
    def __init__(self, value):
        """Initialize the constant return value.

        Parameters
        ----------
        value: scalar
            Constant return value for the function.
        """
        self.value = value

    def exec(self, values):
        """Execute method for the evaluation function. Returns the defined
        constant value.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        return self.value

    def prepare(self, df):
        """Prepare method for the superclass. There is nothing to prepare for
        the constant evaluation function.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.base.EvalFunction
        """
        return self
