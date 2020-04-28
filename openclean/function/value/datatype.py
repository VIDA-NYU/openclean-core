# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of scalar predicates that check if given values belong to
different raw data types.
"""

import math

from datetime import datetime


# -- Type cast function -------------------------------------------------------

class cast(object):
    """Generic type cast function. Attempts to cast values to a type (by using
    a provided callable). If type cast fails either (i.e., the callable raises
    a ValueError) an error is raised or a default value is returned.
    """
    def __init__(self, func, default_value=None, raise_error=False):
        """Initialize the type cast function. Define behavior for those cases
        where type casting fails.

        Parameters
        ----------
        func: callable
            Function that converts the data type of a given scalar value.
        default_value: scalar, optional
            Default value that is being returned for values that cannot be
            casted to the specified type if the raise_error flag is False.
        raise_error: bool, optional
            Raise ValueError if the value that is being extracted from a data
            frame row by the value function cannot be cast to the specified
            type.
        """
        self.func = func
        self.default_value = default_value
        self.raise_error = raise_error

    def __call__(self, value):
        """Convert given argument value by applying the type cast function.

        Parameters
        ----------
        value: scalar
            Scalar value that is being converted to a different data type.

        Returns
        -------
        scalar
        """
        try:
            return self.func(value)
        except ValueError as ex:
            if self.raise_error:
                raise ex
            else:
                return self.default_value


class to_int(cast):
    """Short-cut function to convert scalar values to integer."""
    def __init__(self, default_value=None, raise_error=False):
        """Initialize the type cast function.

        Parameters
        ----------
        default_value: scalar, optional
            Default value that is being returned for values that cannot be
            casted to the specified type if the raise_error flag is False.
        raise_error: bool, optional
            Raise ValueError if the value that is being extracted from a data
            frame row by the value function cannot be cast to the specified
            type.
        """
        super(to_int, self).__init__(
            func=int,
            default_value=default_value,
            raise_error=raise_error
        )


class to_float(cast):
    """Short-cut function to convert scalar values to float."""
    def __init__(self, default_value=None, raise_error=False):
        """Initialize the type cast function.

        Parameters
        ----------
        default_value: scalar, optional
            Default value that is being returned for values that cannot be
            casted to the specified type if the raise_error flag is False.
        raise_error: bool, optional
            Raise ValueError if the value that is being extracted from a data
            frame row by the value function cannot be cast to the specified
            type.
        """
        super(to_float, self).__init__(
            func=float,
            default_value=default_value,
            raise_error=raise_error
        )


# -- Type check functions -----------------------------------------------------

class is_date(object):
    """Test if a given string value can be converted into a datetime object for
    a given data format.
    """
    def __init__(self, format):
        """Initialize the date format and the type cast flag.

        Parameters
        ----------
        format: string or list(string)
            Date format string using Python strptime() format directives. This
            can be a list of date formats.
        """
        self.format = format

    def __call__(self, value):
        """Test if a given value can be converted to a data for the format that
        was given at object instantiation.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being a date.

        Returns
        -------
        bool
        """
        # Ensure that the value is of type string to avoid TypeError.
        if not isinstance(value, str):
            value = str(value)
        # Try to convert the given string to a datatime object with the format
        # that was specified at object instantiation. This will raise an
        # exception if the value does not match the datetime format string.
        # Duplicate code depending on whether format is a list of a string.
        if isinstance(self.format, list):
            for date_format in self.format:
                try:
                    datetime.strptime(value, date_format)
                    return True
                except ValueError:
                    pass
            return False
        else:
            try:
                datetime.strptime(value, self.format)
                return True
            except ValueError:
                return False


class is_float(object):
    """Test if a given value is of type Float."""
    def __init__(self, typecast=True):
        """Initialize the type cast flag. If the flag is True an attempt will
        be made to cast any string value to float.

        Parameters
        ----------
        typecast: bool, optional
            Cast string values to float if True.
        """
        self.typecast = typecast

    def __call__(self, value):
        """Test if a given value is of type float. If the type cast flag is
        True, any string value that can successfully be converted to a float
        will also be accepted.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being a float.

        Returns
        -------
        bool
        """
        datatype = type(value)
        if datatype == float:
            return True
        elif datatype != str or not self.typecast:
            return False
        # Attempt to cast a string value to float. Return True is successful.
        try:
            float(value)
        except ValueError:
            return False
        return True


class is_int(object):
    """Test if a given value is of type Integer."""
    def __init__(self, typecast=True):
        """Initialize the type cast flag. If the flag is True an attempt will
        be made to cast any string value to int.

        Parameters
        ----------
        typecast: bool, optional
            Cast string values to float if True.
        """
        self.typecast = typecast

    def __call__(self, value):
        """Test if a given value is of type int. If the type cast flag is
        True, any string value that can successfully be converted to an integer
        will also be accepted.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being an integer.

        Returns
        -------
        bool
        """
        datatype = type(value)
        if datatype == int:
            return True
        elif datatype != str or not self.typecast:
            return False
        # Attempt to cast a string value to int. Return True is successful.
        try:
            int(value)
        except ValueError:
            return False
        return True


class is_nan(object):
    """Test if a given value is a number."""
    def __call__(self, value):
        """Return True if the given value is not a number.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being a number.

        Returns
        -------
        bool
        """
        # Math.isnan is only defined for int and float. Will raise a TypeError
        # for values of other types. We consider those values not as numbers.
        try:
            return math.isnan(value)
        except TypeError:
            return False
