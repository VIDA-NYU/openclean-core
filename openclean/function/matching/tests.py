# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper classes and functions for unit tests of different
matcher classes.
"""

from openclean.data.mapping import StringMatch
from openclean.function.matching.base import StringSimilarity

from typing import List, Iterable


class DummyMatcher(StringSimilarity):
    """Dummy string similarity that returns a list of matches that is initialized
    when the object is constructed.
    """
    def __init__(self, matches: List[StringMatch]):
        """Initialize the list of matches that is returned as the result for
        any query string.
        """
        self.result = matches

    def match(self, vocabulary: Iterable[str], query: str) -> List[StringMatch]:
        """Returns the list of matches that was initialized when the object was
        constructed.

        Parameters
        ----------
        vocabulary: Iterable[str]
            List of strings to compare with.
        query: string
            Second argument for similarity score computation - the query term.

        Returns
        -------
        list of openclean.data.mapping.StringMatch
        """
        return self.result
