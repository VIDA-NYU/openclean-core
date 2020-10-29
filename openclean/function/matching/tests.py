# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of helper classes and functions for unit tests of different
matcher classes.
"""

from openclean.function.matching.base import StringSimilarity, StringMatchResult

from typing import List, Iterable

class DummySimilarity(StringSimilarity):
    """Dummy string similarity that uses the normalized difference in value
    length as the similarity score, i.e., min(len(val1, val2) divided by
    max(len(val1, val2)).
    """
    def match(self, vocabulary: Iterable[str], query: str) -> List[StringMatchResult]:
        """Returns the different in length between the query sting and a list of vocabulary as their
        similarity scores.

        Parameters
        ----------
        vocabulary: Iterable[str]
            List of strings to compare with.
        query: string
            Second argument for similarity score computation - the query term.

        Returns
        -------
        List[StringMatchResult]
        """
        matches = list()
        for term in vocabulary:
            length = [len(str(query)), len(str(term))]
            score = min(length) / max(length)
            matches.append((score, term))
        return matches
