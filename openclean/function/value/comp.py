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


# -- Generic compare operator -------------------------------------------------

class Comparison(metaclass=ABCMeta):
    """The generic comparison operator provides functionality to handle cases
    where values of incompatible data types are being compared. An errror will
    be raised for incompatible types if the raise_error flag is True. If the
    flag is False, the comparison will evaluate to False for those values.
    """
    def __init__(self, value, raise_error=False):
        """Initialize the constant values against which all input values are
        being compared.

        Parameters
        ----------
        value: scalar
            Value expression agains which column values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        self.comp_value = value
        self.raise_error = raise_error

    def __call__(self, value):
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
            return self.comp(value)
        except TypeError as ex:
            if self.raise_error:
                raise ex
            else:
                return False

    @abstractmethod
    def comp(self, value):
        """Abstract compare function. Implement functionality depending on the
        type of comparator. All implementations can expect that an attempt has
        been made to cast the data type of the input value if requested by the
        type cast flag.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        raise NotImplementedError()


# -- Implementations for the standard value comparators -----------------------

class eq(Comparison):
    """Simple comparator for single columns values that tests for equality with
    a given constant value.
    """
    def __init__(
        self, value, ignore_case=False, raise_error=False
    ):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        value: scalar
            Constant scalar value against which all input values are compared.
        ignore_case: bool, optional
            Ignore case in comparison if set to True.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(eq, self).__init__(value=value, raise_error=raise_error)
        self.ignore_case = ignore_case

    def comp(self, value):
        """Test for equality between the constant compare value and the given
        input value.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        if self.ignore_case and isinstance(value, str):
            return value.lower() == self.comp_value.lower()
        else:
            return value == self.comp_value


class eq_ignore_case(eq):
    """Shortcut for comparing single column values in a case-insenstive manner.
    """
    def __init__(self, value, raise_error=False):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        value: scalar
            Constant scalar value against which all input values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(eq_ignore_case, self).__init__(value=value, ignore_case=True)


class geq(Comparison):
    """Simple comparator for single column values that tests whether a give
    value is greater or equal than a constant comparison value.
    """
    def __init__(self, value, raise_error=False):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        value: scalar
            Constant scalar value against which all input values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(geq, self).__init__(value=value, raise_error=raise_error)

    def comp(self, value):
        """Test whether a column value is greater or equal than a given compare
        value.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        return value >= self.comp_value


class gt(Comparison):
    """Simple comparator for single column values that tests whether a given
    value is greater than a constant comparison value.
    """
    def __init__(self, value, raise_error=False):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        value: scalar
            Constant scalar value against which all input values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(gt, self).__init__(value=value, raise_error=raise_error)

    def comp(self, value):
        """Test whether a column value is greater than a given compare value.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        return value > self.comp_value


class leq(Comparison):
    """Simple comparator for single column values that tests whether a given
    value is less or equal than a constant comparison value.
    """
    def __init__(self, value, raise_error=False):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        value: scalar
            Constant scalar value against which all input values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(leq, self).__init__(value=value, raise_error=raise_error)

    def comp(self, value):
        """Test whether a column value is less or equal than a given compare
        value.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        return value <= self.comp_value


class lt(Comparison):
    """Simple comparator for single column values that tests whether a given
    value is less than a constant comparison value.
    """
    def __init__(self, value, raise_error=False):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        value: scalar
            Constant scalar value against which all input values are compared.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(lt, self).__init__(value=value, raise_error=raise_error)

    def comp(self, value):
        """Test whether a column value is less than a given compare value.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        return value < self.comp_value


class neq(Comparison):
    """Simple comparator for single olumn values that tests for inequality with
    a constant comparison value.
    """
    def __init__(self, value, ignore_case=False, raise_error=False):
        """Initialize the constant value against which all input values are
        being compared.

        Parameters
        ----------
        value: scalar
            Constant scalar value against which all input values are compared.
        ignore_case: bool, optional
            Ignore case in comparison if set to True.
        raise_error: bool, optional
            Raise TypeError exception if values of incompatible data types are
            being compared. By default, the comparison result is False.
        """
        super(neq, self).__init__(value=value, raise_error=raise_error)
        self.ignore_case = ignore_case

    def comp(self, value):
        """Test for inequality between the column value and the result of
        evaluating the value expression on a data frame row.

        Parameters
        ----------
        value: scalar
            Scalar value that is compared against the constant compare value.

        Returns
        -------
        bool
        """
        if self.ignore_case and isinstance(value, str):
            return value.lower() != self.comp_value.lower()
        else:
            return value != self.comp_value


# -- Helper methods -----------------------------------------------------------

def get_threshold(threshold):
    """Ensure that the given threshold is a callable.

    Parameters
    ----------
    threshold: callable, int or float
        Expects a callable or a numeric value that will be wrapped in a
        comparison operator.

    Retuns
    ------
    callable

    Raise
    -----
    ValueError
    """
    # If the threshold is an integer or float create a greater than threshold
    # using the value (unless the value is 1 in which case we use eq).
    if type(threshold) in [int, float]:
        if threshold == 1:
            threshold = eq(1)
        else:
            threshold = gt(threshold)
    elif not callable(threshold):
        raise ValueError('invalid threshold constraint')
    return threshold
