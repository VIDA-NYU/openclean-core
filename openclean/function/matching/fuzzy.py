# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Approximate String Matching using the fuzzyset library"""

from fuzzyset import FuzzySet
from typing import List, Iterable

from openclean.function.matching.base import StringSimilarity, StringMatchResult


class FuzzySimilarity(StringSimilarity):
    """FuzzySet implementation for the String Similarity class. This is a simple
    implementation that uses (FuzzySet)[https://github.com/axiak/fuzzyset/]
    to do approximate string matching operations against the provided vocabulary.

    Note: it converts everything to lowercase and removes all punctuation except commas and spaces

    The matcher allows the user to control the list of returned matches
    via two configuration parameters:

    - best_matches_only: If this flag is True only those matches that have the
      highest score will be returned in the result. If the flag is True all
      matches with a score greater than 0 (or the no_match_threshold, see
      below) are returned.
    - no_match_threshold: Score threshold that controls when a similarity score
      is considered a non-match.

    """

    def __init__(
            self,
            **kwargs
    ):
        """Initialize the associated vocabulary, the similarity function, and
        the configuration parameters.

        Parameters
        ----------
        vocabulary: iterable of string (optional)
            List of terms in the associated vocabulary against which query
            strings are matched.
        **kwargs: Optional
            fuzzy set specific parameters
        """
        self.similarity = FuzzySet(iterable=kwargs.get('vocabulary', ()),
                                   gram_size_lower=kwargs.get('gram_size_lower', 2),
                                   gram_size_upper=kwargs.get('gram_size_upper', 3),
                                   use_levenshtein=kwargs.get('use_levenshtein', True),
                                   rel_sim_cutoff=kwargs.get('rel_sim_cutoff', 1))

    def match(self, vocabulary: Iterable[str], query: str) -> List[StringMatchResult]:
        """Compute a fuzzy similarity score for a string against items from a vocabulary iterable.
        A score of 1 indicates an exact match. A score of 0 indicates a no match.

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
        for term in vocabulary:
            self.similarity.add(term)
        return self.similarity.get(query)
