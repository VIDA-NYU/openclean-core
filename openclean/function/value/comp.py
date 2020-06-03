# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of standard comparison operators that compare scalar input values
(e.g., cell values in a data frame column) against a constant value.
"""

from abc import ABCMeta, abstractmethod

from openclean.function.value.base import (
    CallableWrapper, ValueFunction, scalar_pass_through, to_valuefunc
)


# -- Generic compare operator -------------------------------------------------

class Comparison(ValueFunction, metaclass=ABCMeta):
    """The generic comparison operator provides functionality to handle cases
    where values of incompatible data types are being compared. An errror will
    be raised for incompatible types if the raise_error flag is True. If the
    flag is False, the comparison will evaluate to False for those values.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the constant values against which all input values are
        being compared.

        Parameters
        ----------
        args: scalar, callable, or openclean.function.value.base.ValueFunction
            Either one or two constant values or value functions that represent
            the values that are being compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        if len(args) == 1:
            self.left_value = CallableWrapper(scalar_pass_through)
            self.right_value, = args
        elif len(args) == 2:
            self.left_value, self.right_value = args
        else:
            raise ValueError('invalid argument list')
        self.left_value = to_valuefunc(self.left_value)
        self.right_value = to_valuefunc(self.right_value)
        self.raise_error = kwargs.get('raise_error', False)

    @abstractmethod
    def comp(self, left_value, right_value):
        """Abstract compare function that compares two values. Functionality
        depends on the type of comparator.

        Parameters
        ----------
        left_value: scalar
            Left value of the comparison.
        right_value: scalar
            Right value of the comparison.

        Returns
        -------
        bool
        """
        raise NotImplementedError()

    def eval(self, value):
        """Evaluate the compare expression on the given value. If the type cast
        flag is set to True, an attempt is made to cast the input value to the
        type of the constant compare value. If type casting fails, the result
        is False.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        # Call compare method of the implementing subclass. If a TypeError
        # occurs due to incompatible data types the result is False unless the
        # raise type error flag is True.
        try:
            return self.comp(self.left_value(value), self.right_value(value))
        except TypeError as ex:
            if self.raise_error:
                raise ex
            else:
                return False
        except AttributeError:
            return False

    __call__ = eval

    def is_prepared(self):
        """Returns False if either of the expressions needs preparation.

        Returns
        -------
        bool
        """
        return self.left_value.is_prepared() and self.right_value.is_prepared()

    def prepare(self, values):
        """Prepare the value functions for the left hand side and right hand
        side value of the comparison.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.

        Returns
        -------
        openclean.function.value.base.ValueFunction
        """
        if not self.left_value.is_prepared():
            self.left_value.prepate(values)
        if not self.right_value.is_prepared():
            self.right_value.prepare(values)
        return self


# -- Implementations for the standard value comparators -----------------------

class Eq(Comparison):
    """Simple comparator for single columns values that tests for equality with
    a given constant value.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        args: scalar, callable, or openclean.function.value.base.ValueFunction
            Either one or two constant values or value functions that represent
            the values that are being compared.
        ignore_case: bool, default=False
            Ignore case in comparison if set to True.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Eq, self).__init__(*args, **kwargs)
        self.ignore_case = kwargs.get('ignore_case', False)

    def comp(self, left_value, right_value):
        """Test for equality between the constant compare value and the given
        input value.

        Parameters
        ----------
        left_value: scalar
            Left value of the comparison.
        right_value: scalar
            Right value of the comparison.

        Returns
        -------
        bool
        """
        if self.ignore_case:
            return left_value.lower() == right_value.lower()
        else:
            return left_value == right_value


class EqIgnoreCase(Eq):
    """Shortcut for comparing single column values in a case-insenstive manner.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        args: scalar, callable, or openclean.function.value.base.ValueFunction
            Either one or two constant values or value functions that represent
            the values that are being compared.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        kwargs['ignore_case'] = True
        super(EqIgnoreCase, self).__init__(*args, **kwargs)


class Geq(Comparison):
    """Simple comparator for single column values that tests whether a given
    value is greater or equal than a constant comparison value.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        args: scalar, callable, or openclean.function.value.base.ValueFunction
            Either one or two constant values or value functions that represent
            the values that are being compared.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Geq, self).__init__(*args, **kwargs)

    def comp(self, left_value, right_value):
        """Test whether a column value is greater or equal than a given compare
        value.

        Parameters
        ----------
        left_value: scalar
            Left value of the comparison.
        right_value: scalar
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return left_value >= right_value


class Gt(Comparison):
    """Simple comparator for single column values that tests whether a given
    value is greater than a constant comparison value.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        args: scalar, callable, or openclean.function.value.base.ValueFunction
            Either one or two constant values or value functions that represent
            the values that are being compared.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Gt, self).__init__(*args, **kwargs)

    def comp(self, left_value, right_value):
        """Test whether a column value is greater than a given compare value.

        Parameters
        ----------
        left_value: scalar
            Left value of the comparison.
        right_value: scalar
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return left_value > right_value


class Leq(Comparison):
    """Simple comparator for single column values that tests whether a given
    value is less or equal than a constant comparison value.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        args: scalar, callable, or openclean.function.value.base.ValueFunction
            Either one or two constant values or value functions that represent
            the values that are being compared.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Leq, self).__init__(*args, **kwargs)

    def comp(self, left_value, right_value):
        """Test whether a column value is less or equal than a given compare
        value.

        Parameters
        ----------
        left_value: scalar
            Left value of the comparison.
        right_value: scalar
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return left_value <= right_value


class Lt(Comparison):
    """Simple comparator for single column values that tests whether a given
    value is less than a constant comparison value.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        args: scalar, callable, or openclean.function.value.base.ValueFunction
            Either one or two constant values or value functions that represent
            the values that are being compared.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Lt, self).__init__(*args, **kwargs)

    def comp(self, left_value, right_value):
        """Test whether a column value is less than a given compare value.

        Parameters
        ----------
        left_value: scalar
            Left value of the comparison.
        right_value: scalar
            Right value of the comparison.

        Returns
        -------
        bool
        """
        return left_value < right_value


class Neq(Comparison):
    """Simple comparator for single olumn values that tests for inequality with
    a constant comparison value.
    """
    def __init__(self, *args, **kwargs):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        args: scalar, callable, or openclean.function.value.base.ValueFunction
            Either one or two constant values or value functions that represent
            the values that are being compared.
        ignore_case: bool, default=False
            Ignore case in comparison if set to True.
        raise_error: bool, default=False
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(Neq, self).__init__(*args, **kwargs)
        self.ignore_case = kwargs.get('ignore_case', False)

    def comp(self, left_value, right_value):
        """Test for inequality between the column value and the result of
        evaluating the value expression on a data frame row.

        Parameters
        ----------
        left_value: scalar
            Left value of the comparison.
        right_value: scalar
            Right value of the comparison.

        Returns
        -------
        bool
        """
        if self.ignore_case:
            return left_value.lower() != right_value.lower()
        else:
            return left_value != right_value
