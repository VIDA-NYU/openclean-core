# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of scalar predicates that check for empty and null values."""


# -- Empty string or None -----------------------------------------------------

class is_empty(object):
    """Test values for being an empty string or None."""
    def __init__(self, ignore_whitespace=False):
        """Initialize the ignore whitespace flag. If the flag is True, strings
        that are composed only of whitespace characters are also considered as
        empty strings.

        Parameters
        ----------
        ignore_whitespace: bool, optional
            Trim non-None values if the flag is set to True.
        """
        self.ignore_whitespace = ignore_whitespace

    def __call__(self, value):
        """Returns True if the given value is either an empty string or None.
        If the ignore whitespace flag is True any string that only contains
        whitespace characters is also considered empty.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being empty.

        Returns
        -------
        bool
        """
        if value is None:
            return True
        if self.ignore_whitespace and isinstance(value, str):
            return value.strip() == ''
        return value == ''


class is_not_empty(object):
    """Test values for not being an empty string or None."""
    def __init__(self, ignore_whitespace=False):
        """Initialize the ignore whitespace flag. If the flag is True, strings
        that are composed only of whitespace characters are also considered as
        empty strings.

        Parameters
        ----------
        ignore_whitespace: bool, optional
            Trim non-None values if the flag is set to True.
        """
        self.ignore_whitespace = ignore_whitespace

    def __call__(self, value):
        """Returns True if the given value is not None and not an empty string.
        If the ignore whitespace flag is True any string that only contains
        whitespace characters is considered empty.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for not being empty.

        Returns
        -------
        bool
        """
        if value is None:
            return False
        if self.ignore_whitespace and isinstance(value, str):
            return value.strip() != ''
        return value != ''


# --  None --------------------------------------------------------------------

class is_none(object):
    """Test if a given value is None."""
    def __call__(self, value):
        """Returns True if the given value is None.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being None.

        Returns
        -------
        bool
        """
        return value is None


class is_not_none(object):
    """Test if a given value is not None."""
    def __call__(self, value):
        """Returns True if the given value is not None.

        Parameters
        ----------
        value: scalar
            Scalar value that is tested for being not None.

        Returns
        -------
        bool
        """
        return value is not None
