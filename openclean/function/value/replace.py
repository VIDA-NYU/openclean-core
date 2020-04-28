# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Lookup function for value mappings."""

from openclean.function.value.comp import eq


class lookup(object):
    """Dictionary lookup function. Uses a mapping dictionary to convert given
    input values to their pre-defined targets.
    """
    def __init__(
        self, mapping, default_value=None, for_missing='default',
        as_string=False
    ):
        """Initialize the mapping dictionary and properties that control the
        behavior of the lookup function.

        Parameters
        ----------
        mapping: dict
            Mapping of input values to their pre-defined targets
        default_value: scalar, optional
            Default return value for input values that do not have a defined
            mapping (if the raise error flag is False).
        for_missing: string, int or list of int, optional
            Determine the behavior for missing values. Expects either one of
            three values: 'default' returns the default value, 'error' raises a
            KeyError, and 'self' returns the argument. If the argument is of
            type integer (or list of integer) it is assumed that lookup keys
            are tuples and the tuple value(s) identified by the index
            position(s) will be returned.
        as_string: bool, optional
            Convert all input values to string before lookup if True.

        Raises
        ------
        ValueError
        """
        # Ensure that the mapping is a dictionary.
        if not isinstance(mapping, dict):
            raise ValueError('not a dictionary {}'.format(mapping))
        if isinstance(for_missing, str):
            if for_missing not in ['default', 'error', 'self']:
                msg = 'invalid argument {} for missing behavior'
                raise ValueError(msg.format(for_missing))
        elif isinstance(for_missing, list):
            for i in for_missing:
                if not isinstance(i, int):
                    msg = 'invalid argument {} for missing behavior'
                    raise ValueError(msg.format(for_missing))
        elif not isinstance(for_missing, int):
            msg = 'invalid argument {} for missing behavior'
            raise ValueError(msg.format(for_missing))
        self.mapping = mapping
        self.default_value = default_value
        self.for_missing = for_missing
        self.as_string = as_string

    def __call__(self, value):
        """Return the defined target value for a given lookup value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        any
        """
        key = value
        # Convert the lookup key to string if the respective flag is True.
        if self.as_string:
            key = str(key)
        # Raise an error if the raise error flag is True and the given key
        # value is unknown. Otherwise, return the defined target value or the
        # default value (for missing keys).
        if key not in self.mapping:
            if self.for_missing == 'self':
                return key
            elif self.for_missing == 'default':
                return self.default_value
            elif isinstance(self.for_missing, int):
                return key[self.for_missing]
            elif isinstance(self.for_missing, list):
                return tuple([key[i] for i in self.for_missing])
            else:
                raise KeyError('unknown key {}'.format(key))
        return self.mapping.get(key)


class replace(lookup):
    """Replace function that returns a pre-defined replacement value for input
    values that are contained in a dictionary. For all other values the
    original input value is returned. This is a shortcut for a lookup table
    that returns self for missing values.
    """
    def __init__(self, mapping, as_string=False):
        """Initialize the object properties.

        Parameters
        ----------
        mapping: dict
            Mapping of input values to their pre-defined targets
        as_string: bool, optional
            Convert all input values to string before lookup if True.
        """
        super(replace, self).__init__(
            mapping=mapping,
            for_missing='self',
            as_string=as_string
        )


class varreplace(object):
    """Replace function for variable argument lists. Returns pre-defined
    replacement value for input values that satisfy a given condition.
    """
    def __init__(self, cond, values):
        """Initialize the replacement condition and the replacement values.

        Parameters
        ----------
        cond: callable or class
            Function that is evaluated to identify values that are modified.
        values: constant or callable or class
            Constant return value for modified values or function that is used
            to compute the modified value.
        """
        # Ensure that the condition is callable.
        if isinstance(cond, type):
            cond = cond()
        elif not callable(cond):
            cond = eq(cond)
        self.cond = cond
        # If values is a clas object create an instance of that class.
        if isinstance(values, type):
            values = values()
        self.values = values

    def __call__(self, *args):
        """Return a modified value if the given argument satisfies the
        replacement condition. Otherwise, the value is returned as is.

        Parameters
        ----------
        args: lists
            Variable list of argument values.

        Returns
        -------
        list
        """
        if self.cond(*args):
            return self.values(*args) if callable(self.values) else self.values
        return args
