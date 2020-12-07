# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper functions for strings."""

from typing import Any


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
    return str(value).lower()


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
    return str(value).upper()


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
    return str(value).title()
