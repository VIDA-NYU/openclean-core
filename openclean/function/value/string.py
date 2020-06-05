# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions that operate on scalara values from cells in data
frame rows.
"""

from openclean.function.base import (
    CallableWrapper, PreparedFunction, ValueFunction
)


class StringFunction(ValueFunction):
    """Evaluate a given string function on a given scalar value. This class is
    a wrapper for common string functions that (i) allows to defined behavior
    for arguments that are not strings, and (ii) pass the modified value on
    to a wrapped function to compute the final result.

    The optional consumer function allows to wrap arbitrary functions around
    a string manipulation function. If given, the consumer is called with the
    modified value (after applying the string function) and the consumer result
    is returned as the result of this function.

    For values that are not of type string the default is to not apply the
    string function. Those values are returned (or passed on to the consumer)
    as is. If the as_string flag is set to True the string representation of
    the value is used and the string function is applied. If the raise_error
    flag is True a TypeError is raised. The raise_error flag overrides the
    as_string flag.
    """
    def __init__(
        self, func, consumer=None, as_string=False, raise_error=False
    ):
        """Initialize the object properties.

        Parameters
        ----------
        func: callable
            String function that is executed on given argument values.
        consumer: callable or openclean.function.base.ValueFunction,
            default=None
            Downstream function that is executed on the modified value.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        self.func = func
        if consumer is not None:
            if not isinstance(consumer, ValueFunction):
                consumer = CallableWrapper(consumer)
            self.consumer = consumer
        self.consumer = consumer
        self.as_string = as_string
        self.raise_error = raise_error

    def eval(self, value):
        """Execute the string function on the given argument. If the argument
        is not of type string one of three behaviors will occur: (i) a
        TypeError is raised if the raise_error flag is True, (ii) the string
        representation of the value is used if as_string is True (and no error
        is raised), or (iii) the value will be returned (or passed on to the
        consumer function) as is.

        Parameters
        ----------
        value: scalar or tuple
            Scalar value (or tuple of scalar values) that is modified by the
            string function.

        Returns
        -------
        scalar or tuple

        Raises
        ------
        TypeError
        """
        # Handle argument values that are not of type string.
        if type(value) in [list, tuple]:
            value = tuple(self.transform(v) for v in value)
        else:
            value = self.transform(value)
        # Call the consumer if given
        if self.consumer is not None:
            return self.consumer.eval(value)
        else:
            return value

    __call__ = eval

    def is_prepared(self):
        """Returns False if the optional consumer requires preparation.
        Otherwise, no preparation is required.

        Returns
        -------
        bool
        """
        return self.consumer.is_prepared() if self.consumer else True

    def prepare(self, values):
        """Optional step to prepare the function for a given list of values.
        This step is only relevant for a potential consumer.

        Parameters
        ----------
        values: list
            List of scalar values or tuples of scalar values.
        """
        if self.consumer is not None:
            return StringFunction(
                func=self.func,
                consumer=self.consumer.prepare(values),
                as_string=self.as_string,
                raise_error=self.raise_error
            )
        return self

    def transform(self, value):
        """Apply the string function on a single scalar value. Raises a
        ValueError if the value is not of type string and the raise_error flag
        is True.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being a domain member.

        Returns
        -------
        scalar
        """
        if not isinstance(value, str):
            if self.raise_error:
                raise ValueError('invalid argument {}'.format(value))
            elif self.as_string:
                return self.func(str(value))
            return value
        return self.func(value)


# -- Shortcuts for common string manipulation functions -----------------------

class Capitalize(StringFunction):
    """String function that capitalizes the first letter in argument values."""
    def __init__(self, consumer=None, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        consumer: callable
            Downstream function that is executed on the modified value.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        super(Capitalize, self).__init__(
            func=str.capitalize,
            consumer=consumer,
            as_string=as_string,
            raise_error=raise_error
        )


class Lower(StringFunction):
    """String function that converts argument values to lower case."""
    def __init__(self, consumer=None, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        consumer: callable
            Downstream function that is executed on the modified value.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        super(Lower, self).__init__(
            func=str.lower,
            consumer=consumer,
            as_string=as_string,
            raise_error=raise_error
        )


class Split(StringFunction):
    """String function that splits a given string based on a given delimiter
    character.
    """
    def __init__(self, delim, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        delim: string, optional
            Delimiter string.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        super(Split, self).__init__(
            func=lambda x: x.split(delim),
            as_string=as_string,
            raise_error=raise_error
        )


class Tokens(PreparedFunction):
    """String function that splits a given string based on a given delimiter
    and evaluates the given comparator on the length of the returned value.
    """
    def __init__(self, delim, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        delim: string, optional
            Delimiter string.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        self.split = Split(delim, as_string=as_string, raise_error=raise_error)

    def eval(self, value):
        """Split the given value and evaluate the compare operator on the
        length of the returned token list.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being a domain member.

        Returns
        -------
        bool
        """
        tokens = self.split(value)
        # The returned value may not be a list for values that are not of type
        # string. In this case they are assumed scalar values of length 1.
        try:
            length = len(tokens)
        except TypeError:
            length = 1
        return length


class Upper(StringFunction):
    """String function that converts argument values to upper case."""
    def __init__(self, consumer=None, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        consumer: callable
            Downstream function that is executed on the modified value.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        super(Upper, self).__init__(
            func=str.upper,
            consumer=consumer,
            as_string=as_string,
            raise_error=raise_error
        )
