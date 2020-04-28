# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions that operate on scalara values from cells in data
frame rows.
"""

from openclean.function.value.comp import eq


class StringFunction(object):
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
        consumer: callable, optional
            Downstream function that is executed on the modified value.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        self.func = func
        self.consumer = consumer
        self.as_string = as_string
        self.raise_error = raise_error

    def __call__(self, value):
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
            value = tuple(self.apply(v) for v in value)
        else:
            value = self.apply(value)
        # Call the consumer if given
        if self.consumer is not None:
            return self.consumer(value)
        else:
            return value

    def apply(self, value):
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

class capitalize(StringFunction):
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
        super(capitalize, self).__init__(
            func=str.capitalize,
            consumer=consumer,
            as_string=as_string,
            raise_error=raise_error
        )


class lower(StringFunction):
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
        super(lower, self).__init__(
            func=str.lower,
            consumer=consumer,
            as_string=as_string,
            raise_error=raise_error
        )


class split(StringFunction):
    """String function that splits a given string based on a given delimiter
    character.
    """
    def __init__(self, sep, validate=None, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        sep: string, optional
            Delimiter string.
        validate: int or callable, optional
            Validate the number of generated tokens against the given count or
            predicate. Raises ValueError if the validation fails.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        if validate is not None:
            super(split, self).__init__(
                func=split_and_validate(sep, validate),
                as_string=as_string,
                raise_error=raise_error
            )
        else:
            super(split, self).__init__(
                func=lambda x: x.split(sep),
                as_string=as_string,
                raise_error=raise_error
            )


class tokens(object):
    """String function that splits a given string based on a given delimiter
    and evaluates the given comparator on the length of the returned value.
    """
    def __init__(self, sep, validate, as_string=False, raise_error=False):
        """Initialize the object properties.

        Parameters
        ----------
        sep: string, optional
            Delimiter string.
        validate: int or callable
            Compare predicate that is evaluated on the number of tokens
            returned by the split funciton.
        as_string: bool, optional
            Use string representation for non-string values.
        raise_error: bool, optional
            Raise TypeError for non-string arguments.
        """
        self.split = split(sep, as_string=as_string, raise_error=raise_error)
        self.validate = validate if callable(validate) else eq(validate)

    def __call__(self, value):
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
        return self.validate(length)


class upper(StringFunction):
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
        super(upper, self).__init__(
            func=str.upper,
            consumer=consumer,
            as_string=as_string,
            raise_error=raise_error
        )


# -- Helper Functions ---------------------------------------------------------

class split_and_validate(object):
    """Helper function to split a given string and validate the number of
    returned tokens.
    """
    def __init__(self, sep, validate):
        """Initialize the split delimiter and the token count validator.

        Parameters
        ----------
        sep: string, optional
            Delimiter string.
        validate: int or callable
            Compare predicate that is evaluated on the number of tokens
            returned by the split funciton.
        """
        self.sep = sep
        self.validate = validate if callable(validate) else eq(validate)

    def __call__(self, value):
        """Split the given string and validate the number of tokens.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being a domain member.

        Returns
        -------
        list

        Raises
        ------
        ValueError
        """
        tokens = value.split(self.sep)
        try:
            length = len(tokens)
        except TypeError:
            length = 1
        if not self.validate(length):
            raise ValueError('unexpected number of tokens {}'.format(length))
        return tokens
