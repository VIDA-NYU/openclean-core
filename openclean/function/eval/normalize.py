# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Normalize values in a data frame column."""

from openclean.function.eval.base import Eval
from openclean.util.core import scalar_pass_through

import openclean.function.value.normalize as norm


# -- Generic normalization function -------------------------------------------

class Normalize(Eval):
    """Normalization function for values in a data frame column."""
    def __init__(self, columns, normalizer):
        """Create an instance of an evaluation function that normalizes values
        in a data frame column.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        normalizer: openclean.function.value.base.ValueFunction
            Function that is used to normalize values.
        """
        super(Normalize, self).__init__(
            func=normalizer,
            columns=columns,
            is_unary=True
        )


# -- Specific normalization functions -----------------------------------------

class DivideByTotal(Normalize):
    """Divide values in a list by the sum over all values."""
    def __init__(
        self, columns, raise_error=True, default_value=scalar_pass_through
    ):
        """Initialize the raise error flag and the default value that determine
        the behavior for non-numeric values.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        default_value: scalar, tuple, or callable, default=scalar_pass_through
            Value (or function) that is used (evaluated) as substitute for
            non-numeric values if no error is raised. By default, a value is
            returned as is.
        sum: float or int, default=None
            Pre-computed sum of values. If not set this should be computed by
            the prepare function.
        """
        super(DivideByTotal, self).__init__(
            columns=columns,
            normalizer=norm.DivideByTotal(
                raise_error=raise_error,
                default_value=default_value
            )
        )


class MaxAbsScale(Normalize):
    """Divided values in a list by the absolute maximum over all values."""
    def __init__(
        self, columns, raise_error=True, default_value=scalar_pass_through
    ):
        """Initialize the raise error flag and the default value that determine
        the behavior for non-numeric values.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        default_value: scalar, tuple, or callable, default=scalar_pass_through
            Value (or function) that is used (evaluated) as substitute for
            non-numeric values if no error is raised. By default, a value is
            returned as is.
        maximum: float or int, default=None
            Pre-computed maximum of values. If not set this should be computed
            by the prepare function.
        """
        super(MaxAbsScale, self).__init__(
            columns=columns,
            normalizer=norm.MaxAbsScale(
                raise_error=raise_error,
                default_value=default_value
            )
        )


class MinMaxScale(Normalize):
    """Normalize values in a list using min-max feature scaling."""
    def __init__(
        self, columns, raise_error=True, default_value=scalar_pass_through
    ):
        """Initialize the raise error flag and the default value that determine
        the behavior for non-numeric values.

        Parameters
        ----------
        columns: int, string, openclean.function,.base.EvalFunction, or list
            Single column or list of column index positions or column names.
            This can also be a single evalaution function or a list of
            functions.
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        default_value: scalar, tuple, or callable, default=scalar_pass_through
            Value (or function) that is used (evaluated) as substitute for
            non-numeric values if no error is raised. By default, a value is
            returned as is.
        minumum: float or int, default=None
            Pre-computed minimum of values. If not set this should be computed
            by the prepare function.
        maximum: float or int, default=None
            Pre-computed maximum of values. If not set this should be computed
            by the prepare function.
        """
        super(MinMaxScale, self).__init__(
            columns=columns,
            normalizer=norm.MinMaxScale(
                raise_error=raise_error,
                default_value=default_value
            )
        )
