# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Lookup function for value mappings."""

from openclean.function.base import PreparedFunction


class Lookup(PreparedFunction):
    """Dictionary lookup function. Uses a mapping dictionary to convert given
    input values to their pre-defined targets.
    """
    def __init__(
        self, mapping, raise_error=False, default_value=None, as_string=False
    ):
        """Initialize the mapping dictionary and properties that control the
        behavior of the lookup function.

        Parameters
        ----------
        mapping: dict
            Mapping of input values to their pre-defined targets
        raise_error: bool, default=False
            Raise ValueError if a value given as argument to the eval function
            is not contained in the mapping.
        default_value: scalar, default=None
            Default return value for input values that are not contained in the
            mapping (if the raise error flag is False).
        as_string: bool, optional
            Convert all input values to string before lookup if True.

        Raises
        ------
        ValueError
        """
        # Ensure that the mapping is a dictionary.
        if not isinstance(mapping, dict):
            raise ValueError('not a dictionary {}'.format(mapping))
        self.mapping = mapping
        self.raise_error = raise_error
        self.default_value = default_value
        self.as_string = as_string

    def eval(self, value):
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
        if key not in self.mapping:
            # Raise an error if the raise error flag is True and the given key
            # value is unknown. Otherwise, return the defined target value or
            # the default value (for missing keys).
            if self.raise_error:
                raise KeyError('unknown value {}'.format(value))
            if callable(self.default_value):
                return self.default_value(value)
            return self.default_value
        return self.mapping.get(key)
