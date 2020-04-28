# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Predicates that test whether a regular expression matches a given input
string.
"""

import re


class is_match(object):
    """Match strings against a given regular expression."""
    def __init__(self, pattern, fullmatch=False, as_string=True):
        """Initialize the regular expression pattern. The full match flag
        determines whether the pattern has to match input strings completely
        or only partially. The type case flag determines whether values that
        are not of type string are converted to string or ignored.

        Parameters
        ----------
        pattern: string
            Regular expression.
        fullmatch: bool, optional
            If True, the pattern has to match a given string fully in order for
            the predicate to evaluate to True.
        as_string: bool, optional
            Convert values that are not of type string to string if True.
        """
        self.prog = re.compile(pattern)
        self.fullmatch = fullmatch
        self.as_string = as_string

    def __call__(self, value):
        """Match the regular expression against the given string. If the value
        is not of type string it is converted to string if the type case flag
        is True. Otherwise, the result is False.

        Parameters
        ----------
        value: string
            Input value that is matched against the regular expression.

        Returns
        -------
        bool
        """
        # Ensure that the value is of type string to avoid TypeError.
        if not isinstance(value, str):
            if self.as_string:
                value = str(value)
            else:
                return False
        # Use search or fullmatch depending on the value of the full match
        # flag.
        if self.fullmatch:
            return self.prog.fullmatch(value) is not None
        else:
            return self.prog.search(value) is not None


class is_not_match(is_match):
    """Match strings against a given regular expression. Returns True if the
    value does not match the expression.
    """
    def __init__(self, pattern, fullmatch=False, as_string=True):
        """Initialize the regular expression pattern. The full match flag
        determines whether the pattern has to match input strings completely
        or only partially. The type case flag determines whether values that
        are not of type string are converted to string or ignored.

        Parameters
        ----------
        pattern: string
            Regular expression.
        fullmatch: bool, optional
            If True, the pattern has to match a given string fully in order for
            the predicate to evaluate to True.
        as_string: bool, optional
            Convert values that are not of type string to string if True.
        """
        self.prog = re.compile(pattern)
        self.fullmatch = fullmatch
        self.as_string = as_string

    def __call__(self, value):
        """Match the regular expression against the given string. If the value
        is not of type string it is converted to string if the type case flag
        is True. Otherwise, the result is False.

        Parameters
        ----------
        value: string
            Input value that is matched against the regular expression.

        Returns
        -------
        bool
        """
        # Ensure that the value is of type string to avoid TypeError.
        if not isinstance(value, str):
            if self.as_string:
                value = str(value)
            else:
                return True
        # Use search or fullmatch depending on the value of the full match
        # flag.
        if self.fullmatch:
            return self.prog.fullmatch(value) is None
        else:
            return self.prog.search(value) is None
