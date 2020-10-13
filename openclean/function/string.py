# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper functions for strings."""


def to_lower(value):
    """Convert value to lower case stirng. Handles cases where value is not a
    string.

    Parameters
    ----------
    value: any
        Value that is converted to lower case strng.

    Returns
    -------
    string
    """
    try:
        return value.lower()
    except AttributeError:
        return str(value).lower()


def to_upper(value):
    """Convert value to upper case stirng. Handles cases where value is not a
    string.

    Parameters
    ----------
    value: any
        Value that is converted to upper case strng.

    Returns
    -------
    string
    """
    try:
        return value.upper()
    except AttributeError:
        return str(value).upper()
