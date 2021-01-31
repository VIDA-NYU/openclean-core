# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of evaluation functions that return a computed statistic over
one or more data frame columns for all data frame rows.
"""

from typing import Callable, List, Optional, Union

import numpy as np

from openclean.data.types import Value

from openclean.function.eval.base import Eval, InputColumn
from openclean.function.value.base import ConstantValue, ValueFunction


# -- Generic prepared statistics function -------------------------------------

class ColumnAggregator(ValueFunction):
    """Value function that computes an aggregate over a list of values. The
    aggregated value is computed when the function is prepared. It then returns
    a constant value function that is initialized with the aggregation result,
    i.e., that will return the aggregation result for any input value.
    """
    def __init__(self, func: Callable):
        """Initialize the aggregation function.

        Parameters
        ----------
        func: callable
            Function that computes an aggregated value over a list of values.
        """
        self.func = func

    def eval(self, value: Value):
        """Raises an error. The column aggregator can only be used to prepare
        a constant value funciton.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Raises
        -------
        NotImplementedError
        """
        raise NotImplementedError()

    def is_prepared(self) -> bool:
        """The column aggregator has to be prepared.

        Returns
        -------
        bool
        """
        return False

    def prepare(self, values: List[Value]) -> ConstantValue:
        """Optional step to prepare the function for a given set of values.
        This step allows to compute additional statistics over the set of
        values.

        While it is likely that the given set of values represents the values
        for which the eval() function will be called, this property is not
        guaranteed.

        Parameters
        ----------
        values: dict
            Set of distinct scalar values or tuples of scalar values that are
            mapped to their respective frequency count.

        Returns
        -------
        openclean.function.value.base.ConstantValue
        """
        return ConstantValue(self.func(values))


# -- Shortcuts for common statistics methods ----------------------------------

class Avg(Eval):
    """Evaluation function that returns the mean of values for one or more
    columns in a data frame.
    """
    def __init__(self, columns: Union[InputColumn, List[InputColumn]]):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        """
        super(Avg, self).__init__(
            columns=columns,
            func=ColumnAggregator(np.mean),
            is_unary=True
        )


class Count(Eval):
    """Evaluation function that counts the number of values in one or more
    columns that match a given value.
    """
    def __init__(
        self, columns: Union[InputColumn, List[InputColumn]],
        value: Optional[Value] = True
    ):
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

        super(Count, self).__init__(
            columns=columns,
            func=ColumnAggregator(count),
            is_unary=True
        )


class Max(Eval):
    """Evaluation function that returns the maximum of values for one or more
    columns in a data frame.
    """
    def __init__(self, columns: Union[InputColumn, List[InputColumn]]):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        """

        def maxval(values: List[Value]) -> Value:
            """Ensure to return a default value for empty lists."""
            return max(values, default=None)

        super(Max, self).__init__(
            columns=columns,
            func=ColumnAggregator(maxval),
            is_unary=True
        )


class Min(Eval):
    """Evaluation function that returns the minimum of values for one or more
    columns in a data frame.
    """
    def __init__(self, columns: Union[InputColumn, List[InputColumn]]):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        """

        def minval(values: List[Value]) -> Value:
            """Ensure to return a default value for empty lists."""
            return min(values, default=None)

        super(Min, self).__init__(
            columns=columns,
            func=ColumnAggregator(minval),
            is_unary=True
        )


class Sum(Eval):
    """Evaluation function that returns the sum over values for one or more
    columns in a data frame.
    """
    def __init__(self, columns: Union[InputColumn, List[InputColumn]]):
        """Initialize the statistics function in the super class as well as the
        list of columns on which the function will be applied.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        """
        super(Sum, self).__init__(
            columns=columns,
            func=ColumnAggregator(sum),
            is_unary=True
        )
