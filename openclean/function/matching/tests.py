# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper classes and functions for unit tests of different
matcher classes.
"""

from openclean.function.matching.base import StringMatcher


class DummyStringMatcher(StringMatcher):
    """Dummy string matcher that uses the normalized difference in value
    length as the similarity score, i.e., min(len(val1, val2) divided by
    max(len(val1, val2)).
    """
    def match(self, val1: str, val2: str) -> float:
        """Returns the different in length between the two stings as their
        similarity score.

        Parameters
        ----------
        val1: string
            First argument for similarity score computation.
        val2: string
            Second argument for similarity score computation.

        Returns
        -------
        float
        """
        length = [len(val1), len(val2)]
        return min(length)/max(length)
