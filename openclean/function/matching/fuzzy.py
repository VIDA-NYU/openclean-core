# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Approximate String Matching using the fuzzyset library."""

from fuzzyset import FuzzySet
from typing import Iterable, List, Optional

from openclean.data.mapping import StringMatch
from openclean.function.matching.base import StringSimilarity


class FuzzySimilarity(StringSimilarity):
    """FuzzySet implementation for the String Similarity class. This is a simple
    implementation that uses (FuzzySet)[https://github.com/axiak/fuzzyset/]
    to do approximate string matching operations against the provided vocabulary.

    Note: it converts everything to lowercase and removes all punctuation except
    commas and spaces.
    """

    def __init__(
        self, vocabulary: Optional[Iterable[str]] = None,
        gram_size_lower: Optional[int] = 2, gram_size_upper: Optional[int] = 3,
        use_levenshtein: Optional[bool] = True, rel_sim_cutoff: Optional[float] = 1.
    ):
        """Initialize the associated vocabulary, and  configuration parameters
        for the fuzzy matcher.

        Parameters
        ----------
        vocabulary: iterable of string (optional)
            List of terms in the associated vocabulary against which query
            strings are matched.
        gram_size_lower: int, default=2
            Lower boundary for n of created n-grames for each value.
        gram_size_upperL int, default=3
            Upper boundary for n of created n-grames for each value.
        use_levenshtein: bool, default=True
            Compute edit distance for metahced terms and return top-50 matches
            based on edit distance.
        rel_sim_cutoff: float, default=1.
            Threshold for matched terms that are considered exact matches.
        """
        self.similarity = FuzzySet(
            iterable=vocabulary if vocabulary is not None else [],
            gram_size_lower=gram_size_lower,
            gram_size_upper=gram_size_upper,
            use_levenshtein=use_levenshtein,
            rel_sim_cutoff=rel_sim_cutoff
        )

    def match(self, vocabulary: Iterable[str], query: str) -> List[StringMatch]:
        """Compute a fuzzy similarity score for a string against items from a
        vocabulary iterable.

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
        for term in vocabulary:
            self.similarity.add(term)
        return [StringMatch(term=t, score=s) for s, t in self.similarity.get(query)]
