# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of unary functions for checking and converting the data type of
given scalar values. Includes functions for type case and predicates for type
checking.
"""

import math
import numpy as np

from datetime import datetime
from dateutil.parser import isoparse
from dateutil.tz import UTC


# -- Type cast function -------------------------------------------------------

def cast(value, func, default_value=None, raise_error=False):
    """Generic type cast function. Attempts to cast values to a type (by using
    a provided callable). If type cast fails (i.e., the callable raises a
    aValueError) (i) an error is raised if the raise error flag is True, or
    (ii) a given default value is returned.

    Parameters
    ----------
    value: scalar
        Scalar value that is being converted using the type conversion
        function.
    func: callable
        Function that converts the data type of a given scalar value.
    default_value: scalar, default=None
        Default value that is being returned for values that cannot be
        casted to the specified type if the raise_error flag is False.
    raise_error: bool, default=False
        Raise ValueError if the value that is being extracted from a data
        frame row by the value function cannot be cast to the specified
        type.

    Returns
    -------
    scalar
    """
    try:
        return func(value)
    except ValueError as ex:
        if raise_error:
            raise ex
    return default_value


def to_datetime(value, default_value=None, raise_error=False):
    """Converts a timestamp string in ISO format into a datatime object in
    UTC timezone.

    Parameters
    ----------
    value: string
        String value that is being converted to datetime.
    default_value: scalar, default=None
        Default value that is being returned for values that cannot be
        converted to datetime if the raise_error flag is False.
    raise_error: bool, default=False
        Raise ValueError if the value cannot be converted to datetime.

    Returns
    -------
    datetime.datetime
    """
    try:
        # Parse timestamp in ISO format. Ensure that the timezone is UTC.
        dt = isoparse(value)
        if not dt.tzinfo == UTC:
            dt = dt.astimezone(UTC)
        return dt
    except ValueError as ex:
        if raise_error:
            raise ex
    return default_value


def to_int(value, default_value=None, raise_error=False):
    """Convert a given value to integer. Raises an error if the given value
    cannot be converted to integer and the raise error flag is True. If the
    flag is False, a given default value will be returned for thoses values
    that cannot be converted to integer.

    Parameters
    ----------
    value: scalar
        Scalar value that is being converted to integer.
    default_value: scalar, default=None
        Default value that is being returned for values that cannot be cast to
        integer if the raise_error flag is False.
    raise_error: bool, default=False
        Raise ValueError if the value cannot be cast to integer.
    """
    return cast(
        value,
        func=int,
        default_value=default_value,
        raise_error=raise_error
    )


def to_float(value, default_value=None, raise_error=False):
    """Convert a given value to float. Raises an error if the given value
    cannot be converted to float and the raise error flag is True. If the
    flag is False, a given default value will be returned for thoses values
    that cannot be converted to float.

    Parameters
    ----------
    value: scalar
        Scalar value that is being converted to float.
    default_value: scalar, default=None
        Default value that is being returned for values that cannot be cast to
        float if the raise_error flag is False.
    raise_error: bool, default=False
        Raise ValueError if the value cannot be cast to float.
    """
    return cast(
        value,
        func=float,
        default_value=default_value,
        raise_error=raise_error
    )


# -- Type check functions -----------------------------------------------------

def is_datetime(value, formats=None):
    """Test if a given string value can be converted into a datetime object for
    a given data format. The function accepts a single date format or a list of
    formates. If no format is given, ISO format is assumed as the default.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being a date.
    formats: string or list(string)
        Date format string using Python strptime() format directives. This
        can be a list of date formats.

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
    if formats is None:
        try:
            isoparse(value)
            return True
        except ValueError:
            pass
    elif isinstance(formats, list):
        for date_format in formats:
            try:
                datetime.strptime(value, date_format)
                return True
            except ValueError:
                pass
    else:
        try:
            datetime.strptime(value, formats)
            return True
        except ValueError:
            pass
    return False


def is_float(value, typecast=True):
    """Test if a given value is of type float. If the type cast flag is True,
    any string value that can successfully be converted to float will also be
    accepted.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being a float.
    typecast: bool, default=True
        Cast string values to float if True.

    Returns
    -------
    bool
    """
    if isinstance(value, float) or isinstance(value, np.float):
        return True
    elif not typecast or not isinstance(value, str):
        return False
    # Attempt to cast a string value to float. Return True is successful.
    try:
        float(value)
    except ValueError:
        return False
    return True


def is_int(value, typecast=True):
    """Test if a given value is of type integer. If the type cast flag is True,
    any string value that can successfully be converted to integer will also be
    accepted.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being an integer.
    typecast: bool, default=True
        Cast string values to integer if True.

    Returns
    -------
    bool
    """
    if isinstance(value, int) or isinstance(value, np.integer):
        return True
    elif not typecast or not isinstance(value, str):
        return False
    # Attempt to cast a string value to integer. Return True is successful.
    try:
        int(value)
    except ValueError:
        return False
    return True


def is_nan(value):
    """Test if a given value is a number. Returns True if the given value is
    not a number.

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


def is_numeric(value, typecast=True):
    """Test if a given value is of type integer or float. If the type cast flag
    is True, any string value that can successfully be converted to integer or
    float will also be accepted.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being a number.
    typecast: bool, default=True
        Cast string values to integer or float if True.

    Returns
    -------
    bool
    """
    if isinstance(value, int) or isinstance(value, np.integer):
        return True
    elif isinstance(value, float) or isinstance(value, np.float):
        return True
    elif not typecast or not isinstance(value, str):
        return False
    # Attempt to cast a string value to float. Here we assume that any string
    # representing an integer value can also be cast to float. Return True is
    # successful.
    try:
        float(value)
    except ValueError:
        return False
    return True


def is_numeric_type(value):
    """Test if a given value is of type integer or float (or the numpy
    equivalent). Does not attempt to cast string values.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being a number.

    Returns
    -------
    bool
    """
    return is_numeric(value, typecast=False)
