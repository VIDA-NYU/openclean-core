# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions to normalize values in a list (e.g., a data frame
column).
"""

from abc import abstractmethod

from openclean.function.value.base import ValueFunction, scalar_pass_through
from openclean.function.value.datatype import is_numeric_type
from openclean.function.value.filter import filter


# -- Generic base class for normalization functions ---------------------------

class NormalizeFunction(ValueFunction):
    """Abstract base class form normalization functions. Implementing classes
    need to implement the compute and prepare methods.
    """
    def __init__(self, raise_error=True, default_value=scalar_pass_through):
        """Initialize the raise error flag and the default value that determine
        the behavior for non-numeric values.

        Parameters
        ----------
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        default_value: scalar, tuple, or callable, default=scalar_pass_through
            Value (or function) that is returned (evaluated) for non-numeric
            values if no error is raised. By default, a value is returned as
            is.
        """
        self.raise_error = raise_error
        self.default_value = default_value

    @abstractmethod
    def compute(self, value):
        """Individual normalization function that is dependent on the
        implementing sub-class. At this point it is assumed that the argument
        value is numeric.

        Parameters
        ----------
        value: scalar
            Scalar value from the list that was used to prepare the function.

        Returns
        -------
        float
        """
        raise NotImplementedError()

    def eval(self, value):
        """Normalize a given value by calling the compute function of the
        implementing class.

        If the given value is not a numeric value either a ValueError is raised
        if the respective flag is True or the default value is returned.

        Parameters
        ----------
        value: scalar
            Scalar value from the list that was used to prepare the function.

        Returns
        -------
        float
        """
        if not is_numeric_type(value):
            # Depending on the raise_error flag we either raise an error for
            # non-numeric values or return the default value (this may require
            # to evaluate the default value function).
            if self.raise_error:
                raise ValueError('not a numeric value {}'.format(value))
            if callable(self.default_value):
                return self.default_value(value)
            return self.default_value
        # Divide the value by the _sum that was initialized in the prepare
        # call. If the sum is zero or not defined we return 0.
        return self.compute(value)

    __call__ = eval


# -- Divide by total sum ------------------------------------------------------

def divide_by_total(
    values, raise_error=True, default_value=scalar_pass_through
):
    """Divide values in a list by the sum over all values. Values that are
    not numeric are either replaced with a given default value or an error
    is raised if the raise error flag is True.

    Parameters
    ----------
    value: scalar
        Scalar value from the list that was used to prepare the function.
    raise_error: bool, optional
        Raise ValueError if the list contains values that are not integer
        or float. If False, non-numeric values are ignored.
    default_value: scalar, tuple, or callable, default=scalar_pass_through
        Value (or function) that is used (evaluated) as substitute for
        non-numeric values if no error is raised. By default, a value is
        returned as is.
    """
    norm = DivideByTotal(raise_error=raise_error, default_value=default_value)
    return norm.apply(values)


class DivideByTotal(NormalizeFunction):
    """Divide values in a list by the sum over all values."""
    def __init__(self, raise_error=True, default_value=scalar_pass_through):
        """Initialize the raise error flag and the default value that determine
        the behavior for non-numeric values.

        Parameters
        ----------
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        default_value: scalar, tuple, or callable, default=scalar_pass_through
            Value (or function) that is used (evaluated) as substitute for
            non-numeric values if no error is raised. By default, a value is
            returned as is.
        """
        super(DivideByTotal, self).__init__(
            raise_error=raise_error,
            default_value=default_value
        )
        # The total sum of values in the list is unknown at construction time.
        # The value will be calculated by the prepare method.
        self._sum = None

    def compute(self, value):
        """Divide given value by the pre-computed sum over all values in the
        list. If the sum was zero the result will be zero.

        If the given value is not a numeric value either a ValueError is raised
        if the respective flag is True or the default value is returned.

        Parameters
        ----------
        value: scalar
            Scalar value from the list that was used to prepare the function.

        Returns
        -------
        float
        """
        # Divide the value by the _sum that was initialized in the prepare
        # call. If the sum is zero or not defined we return 0.
        return float(value) / self._sum if self._sum else 0

    def prepare(self, values):
        """Compute the total sum over all values in the givem list.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.
        """
        values = filter(values, is_numeric_type)
        self._sum = float(sum(values))
        return self


# -- Divide by absolute maximum -----------------------------------------------

def max_abs_scale(values, raise_error=True, default_value=scalar_pass_through):
    """Divide values in a list by the absolute maximum over all values. Values
    that are not numeric are either replaced with a given default value or an
    error is raised if the raise error flag is True.

    Parameters
    ----------
    value: scalar
        Scalar value from the list that was used to prepare the function.
    raise_error: bool, optional
        Raise ValueError if the list contains values that are not integer
        or float. If False, non-numeric values are ignored.
    default_value: scalar, tuple, or callable, default=scalar_pass_through
        Value (or function) that is used (evaluated) as substitute for
        non-numeric values if no error is raised. By default, a value is
        returned as is.
    """
    norm = MaxAbsScale(raise_error=raise_error, default_value=default_value)
    return norm.apply(values)


class MaxAbsScale(NormalizeFunction):
    """Divided values in a list by the absolute maximum over all values."""
    def __init__(self, raise_error=True, default_value=scalar_pass_through):
        """Initialize the raise error flag and the default value that determine
        the behavior for non-numeric values.

        Parameters
        ----------
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        default_value: scalar, tuple, or callable, default=scalar_pass_through
            Value (or function) that is used (evaluated) as substitute for
            non-numeric values if no error is raised. By default, a value is
            returned as is.
        """
        super(MaxAbsScale, self).__init__(
            raise_error=raise_error,
            default_value=default_value
        )
        # The maximum value in the list is unknown at construction time. The
        # value will be calculated by the prepare method.
        self._maximum = None

    def compute(self, value):
        """Divide given value by the pre-computed sum over all values in the
        list. If the sum was zero the result will be zero.

        If the given value is not a numeric value either a ValueError is raised
        if the respective flag is True or the default value is returned.

        Parameters
        ----------
        value: scalar
            Scalar value from the list that was used to prepare the function.

        Returns
        -------
        float
        """
        # Divide the value by the maximum that was initialized in the prepare
        # call. If the maximum is zero or not defined we return 0.
        return float(value) / self._maximum if self._maximum else 0

    def prepare(self, values):
        """Compute the total sum over all values in the givem list.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.
        """
        values = filter(values, is_numeric_type)
        self._maximum = float(max(values))
        return self


# -- Min/Max scale ------------------------------------------------------------

def min_max_scale(values, raise_error=True, default_value=scalar_pass_through):
    """Normalize values in a list using min-max feature scaling. Values that
    are not numeric are either replaced with a given default value or an
    error is raised if the raise error flag is True.

    Parameters
    ----------
    value: scalar
        Scalar value from the list that was used to prepare the function.
    raise_error: bool, optional
        Raise ValueError if the list contains values that are not integer
        or float. If False, non-numeric values are ignored.
    default_value: scalar, tuple, or callable, default=scalar_pass_through
        Value (or function) that is used (evaluated) as substitute for
        non-numeric values if no error is raised. By default, a value is
        returned as is.
    """
    norm = MinMaxScale(raise_error=raise_error, default_value=default_value)
    return norm.apply(values)


class MinMaxScale(NormalizeFunction):
    """Normalize values in a list using min-max feature scaling."""
    def __init__(self, raise_error=True, default_value=scalar_pass_through):
        """Initialize the raise error flag and the default value that determine
        the behavior for non-numeric values.

        Parameters
        ----------
        raise_error: bool, optional
            Raise ValueError if the list contains values that are not integer
            or float. If False, non-numeric values are ignored.
        default_value: scalar, tuple, or callable, default=scalar_pass_through
            Value (or function) that is used (evaluated) as substitute for
            non-numeric values if no error is raised. By default, a value is
            returned as is.
        """
        super(MinMaxScale, self).__init__(
            raise_error=raise_error,
            default_value=default_value
        )
        # The minimum and maximum value in the list are unknown at construction
        # time. The values will be calculated by the prepare method.
        self._minimum = None
        self._maximum = None

    def compute(self, value):
        """Normalize value using min-max feature scaling. If the pre-computed
        minimum and maximum for the value list are equal the result will be
        zero.

        Parameters
        ----------
        value: scalar
            Scalar value from the list that was used to prepare the function.

        Returns
        -------
        float
        """
        if self._minimum == self._maximum:
            return 0
        return (float(value) - self._minimum) / (self._maximum - self._minimum)

    def prepare(self, values):
        """Compute the total sum over all values in the givem list.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.
        """
        values = filter(values, is_numeric_type)
        self._minimum = float(min(values))
        self._maximum = float(max(values))
        return self
