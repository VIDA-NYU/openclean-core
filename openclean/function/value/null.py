# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of scalar predicates that check for empty and null values."""


def is_empty(value, ignore_whitespace=False):
    """Test values for being an empty string or None. If the ignore whitespace
    flag is True any string that only contains whitespace characters is also
    considered empty.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being empty.
    ignore_whitespace: bool, optional
        Trim non-None values if the flag is set to True.

    Returns
    -------
    bool
    """
    if value is None:
        return True
    if ignore_whitespace and isinstance(value, str):
        return value.strip() == ''
    return value == ''


def is_not_empty(value, ignore_whitespace=False):
    """Test values for being is not empty string or None. If the ignore
    whitespace flag is True any string that only contains whitespace characters
    is also considered empty.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being empty.
    ignore_whitespace: bool, optional
        Trim non-None values if the flag is set to True.

    Returns
    -------
    bool
    """
    if value is None:
        return False
    if ignore_whitespace and isinstance(value, str):
        return value.strip() != ''
    return value != ''


def is_none(value):
    """Test if a given value is None.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being None.

    Returns
    -------
    bool
    """
    return value is None


def is_not_none(value):
    """Test if a given value is not None.

    Parameters
    ----------
    value: scalar
        Scalar value that is tested for being None.

    Returns
    -------
    bool
    """
    return value is not None
