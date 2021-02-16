# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
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
from dateutil.parser import parse
from typing import Callable, List, Optional, Union

from openclean.data.types import Scalar
from openclean.function.value.classifier import ClassLabel, ValueClassifier


# -- Type cast function -------------------------------------------------------

def cast(
    value: Scalar, func: Callable, default_value: Optional[Scalar] = None,
    raise_error: Optional[bool] = False
) -> Scalar:
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
    except (OverflowError, TypeError, ValueError) as ex:
        if raise_error:
            raise ex
    return default_value


def to_datetime(
    value: Scalar, default_value: Optional[Scalar] = None,
    raise_error: Optional[bool] = False
) -> Scalar:
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
    scalar
    """
    if isinstance(value, datetime):
        return value
    # See below (is_datetime) for more information on our definition of a valid
    # datetime.
    if len(value) >= 6 and has_two_spec_chars(value):
        try:
            return parse(value, fuzzy=False)
        except (OverflowError, TypeError, ValueError) as ex:
            if raise_error:
                raise ex
    elif raise_error:
        raise ValueError("not a valid date '{}'".format(value))
    return default_value


def to_datetime_format(value: Scalar, formats: Optional[Union[str, List[str]]] = None) -> datetime:
    """Convert a given value to a datetime object for a given date format. If a
    list of format specifications is given an attempt is made to convert the
    value for each format (in given order) until the first format for which
    the conversion succeeds. If none of the formats match the given value None
    is returned.

    Parameters
    ----------
    value: scalar
        Scalar value that is converted to a date.
    formats: string or list(string)
        Date format string using Python strptime() format directives. This
        can be a list of date formats.

    Returns
    -------
    datetime.datetime
    """
    if isinstance(formats, list):
        for date_format in formats:
            try:
                return datetime.strptime(value, date_format)
            except ValueError:
                pass
    else:
        try:
            return datetime.strptime(value, formats)
        except ValueError:
            pass


def to_float(
    value: Scalar, default_value: Optional[Scalar] = None,
    raise_error: Optional[bool] = False
) -> Scalar:
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

    Returns
    -------
    scalar
    """
    return cast(
        value,
        func=float,
        default_value=default_value,
        raise_error=raise_error
    )


def to_int(
    value: Scalar, default_value: Optional[Scalar] = None,
    raise_error: Optional[bool] = False
) -> Scalar:
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

    Returns
    -------
    scalar
    """
    return cast(
        value,
        func=int,
        default_value=default_value,
        raise_error=raise_error
    )


def to_string(value: Scalar) -> bool:
    """Type cast function that tests if a given value is of type string.
    Returns the value if it is of type string or None, otherwise.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being a number.

    Returns
    -------
    bool
    """
    if isinstance(value, str):
        return value


# -- Type check functions -----------------------------------------------------


def is_datetime(
    value: Scalar, formats: Optional[Union[str, List[str]]] = None,
    typecast: Optional[bool] = True
) -> bool:
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
    typecast: bool, default=True
        Attempt to parse string values as dates if True.

    Returns
    -------
    bool
    """
    if isinstance(value, datetime):
        return True
    elif not typecast or not isinstance(value, str):
        return False
    # Try to convert the given string to a datatime object with the format
    # that was specified at object instantiation. This will raise an
    # exception if the value does not match the datetime format string.
    # Duplicate code depending on whether format is a list of a string.
    if formats is None:
        # Issue \#39: dateutil.parse (falsely?) identifies the following
        # strings as dates. For column profiling we want to exclude these:
        # 14A; 271 1/2; 41-05; 6-8
        #
        # As a work-around for now we expect a valid date to have at least six
        # characters (one for day, month, two for year and at least two
        # non-alphanumeric characters.
        #
        # An alternative solution was pointed out by @remram44:
        # https://gitlab.com/ViDA-NYU/datamart/datamart/-/blob/39462a5dca533a1e55596ddcbfc0ac7e98dce4de/lib_profiler/datamart_profiler/temporal.py#L63  # noqa: E501
        #
        # All solutions seem to suffer from the problem that values like
        # 152-12 are valid dates (e.g., 152-12-01 in this case) but also
        # valid house numbers, for example. There is no good solution here.
        # For now we go with the assumption that if someone wants to specify
        # a date it should have at least a day, month and year separated by
        # some special (non-alphanumeric) charcater.
        if len(value) >= 6 and has_two_spec_chars(value):
            try:
                parse(value, fuzzy=False)
                return True
            except (OverflowError, TypeError, ValueError):
                pass
    else:
        return to_datetime_format(value=value, formats=formats) is not None
    return False


def is_float(value: Scalar, typecast: Optional[bool] = True) -> bool:
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
    if isinstance(value, float):
        return True
    elif not typecast or not isinstance(value, str):
        return False
    # Attempt to cast a string value to float. Return True is successful.
    try:
        float(value)
    except ValueError:
        return False
    return True


def is_int(value: Scalar, typecast: Optional[bool] = True) -> bool:
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


def is_nan(value: Scalar) -> bool:
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


def is_numeric(
    value: Scalar, typecast: Optional[bool] = True,
    ignore_nan: Optional[bool] = True
) -> bool:
    """Test if a given value is of type integer or float. If the type cast flag
    is True, any string value that can successfully be converted to integer or
    float will also be accepted.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being a number.
    typecast: bool, default=True
        Cast string values to integer or float if True.
    ignore_nan: bool, default=False
        Consider NaN not as numeric if the flag is True

    Returns
    -------
    bool
    """
    if ignore_nan and is_nan(value):
        return False
    if isinstance(value, int) or isinstance(value, np.integer):
        return True
    elif isinstance(value, float):
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


def is_numeric_type(value: Scalar) -> bool:
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
    return is_numeric(value, typecast=False, ignore_nan=False)


# -- Helper Methods -----------------------------------------------------------

def has_two_spec_chars(value: Scalar) -> bool:
    """Returns True if the given string has at least two non-alphanumeric
    characters.

    Parameters
    ----------
    value: scalar
        Scalar value in a data stream.

    Returns
    -------
    bool
    """
    # We only need to encounter two special-characters for the function to
    # return True. The spec_char flag keeps track of whether we encountered
    # one special character so far.
    spec_char = False
    for c in str(value):
        if not c.isalnum():
            # If we encountered a special character before we can return True
            # since we now encountered two.
            if spec_char:
                return True
            # We encountered the first special character if we reach this
            # point.
            spec_char = True
    return False


# -- Default data type classifiers --------------------------------------------

class Datetime(ClassLabel):
    """Class label assigner for datetime values."""
    def __init__(
        self, label: Optional[str] = 'datetime',
        formats: Optional[Union[str, List[str]]] = None,
        typecast: Optional[bool] = True
    ):
        """Initialize the class label and set the type cast flag.

        Parameters
        ----------
        label: string, default='datetime'
            Label that is returned for values that satisfy the predicate.
        formats: string or list(string)
            Date format string using Python strptime() format directives. This
            can be a list of date formats.
        typecast: bool, default=True
            Parse string values as dates if True.
        """

        def predicate(value):
            return is_datetime(value, formats=formats, typecast=typecast)

        super(Datetime, self).__init__(predicate=predicate, label=label)


class Float(ClassLabel):
    """Class label assigner for float values."""
    def __init__(
        self, label: Optional[str] = 'float', typecast: Optional[bool] = True
    ):
        """Initialize the class label and set the type cast flag.

        Parameters
        ----------
        label: string, default='float'
            Label that is returned for values that satisfy the predicate.
        typecast: bool, default=True
            Cast string values to float if True.
        """

        def predicate(value):
            return is_float(value, typecast=typecast)

        super(Float, self).__init__(predicate=predicate, label=label)


class Int(ClassLabel):
    """Class label assigner for integer values."""
    def __init__(
        self, label: Optional[str] = 'int', typecast: Optional[bool] = True
    ):
        """Initialize the class label and set the type cast flag.

        Parameters
        ----------
        label: string, default='int'
            Label that is returned for values that satisfy the predicate.
        typecast: bool, default=True
            Cast string values to integer if True.
        """

        def predicate(value):
            return is_int(value, typecast=typecast)

        super(Int, self).__init__(predicate=predicate, label=label)


# -- Default classifier -------------------------------------------------------

def DefaultDatatypeClassifier() -> ValueClassifier:
    """Return an instance of the avlue classifier initialized with a default
    set of class labels.

    Returns
    -------
    openclean.function.value.classifier.ValueClassifier
    """
    return ValueClassifier(
        Int(),
        Float(),
        Datetime(),
        default_label='text'
    )
