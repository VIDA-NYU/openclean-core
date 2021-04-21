# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper functions for strings."""

from typing import Any

from openclean.data.types import Value
from openclean.function.value.base import PreparedFunction


class AlphaNumeric(PreparedFunction):
    """Predicate to test whether a given string contains only alpha-numeric
    characters.
    """
    def eval(self, value: Value) -> bool:
        """Returns True if the string representation of a given value contains
        only alpha-numeric characters.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        bool
        """
        # Ensure that the given value is a string.
        value = value if isinstance(value, str) else str(value)
        for c in value:
            if not c.isalpha() and not c.isdigit():
                return False
        return True


# -- Helper Functions ---------------------------------------------------------

def to_len(value: Any) -> int:
    """Get the length of a value. The value is converted to string and the
    number of characters in the resulting string is returned.

    Parameters
    ----------
    value: any
        Value whose length is returned.

    Returns
    -------
    int
    """
    return len(str(value))


def to_lower(value: Any) -> str:
    """Convert value to lower case stirng. Handles cases where value is not a
    string.

    Parameters
    ----------
    value: any
        Value that is converted to lower case string.

    Returns
    -------
    string
    """
    try:
        return str.lower(value)
    except TypeError:
        return value


def to_upper(value: Any) -> str:
    """Convert value to upper case stirng. Handles cases where value is not a
    string.

    Parameters
    ----------
    value: any
        Value that is converted to upper case string.

    Returns
    -------
    string
    """
    try:
        return str.upper(value)
    except TypeError:
        return value


def to_title(value: Any) -> str:
    """Convert value to title case stirng. Handles cases where value is not a
    string.

    Parameters
    ----------
    value: any
        Value that is converted to title case string.

    Returns
    -------
    string
    """
    try:
        return str.title(value)
    except TypeError:
        return value
