# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Approximate String Matching using the fuzzyset library"""

from fuzzyset import FuzzySet
from typing import List, Iterable, Optional

from openclean.function.matching.base import VocabularyMatcher, StringMatchResult


class FuzzyVocabularyMatcher(VocabularyMatcher):
    """FuzzySet implementation for the vocabulary matcher. This is a simple
    implementation that uses (FuzzySet)[https://github.com/axiak/fuzzyset/]
    to do approximate string matching operations using the provided vocabulary.

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
            vocabulary: Iterable[str],
            best_matches_only: Optional[bool] = True,
            no_match_threshold: Optional[float] = 0.,
            cache_results: Optional[bool] = True,
            **kwargs
    ):
        """Initialize the associated vocabulary, the similarity function, and
        the configuration parameters.

        Parameters
        ----------
        vocabulary: iterable of string
            List of terms in the associated vocabulary agains which query
            strings are matched.
        matcher: openclean.function.matching.base.StringMatcher
            String similarity function that is used to compute scores between
            a query string and the values in the vocabulary.
        best_matches_only: bool, default=False
            If True, only matches with the highest score are returned.
        no_match_threshold: float, default=0.
            If the similarity score for a match with a query string is below
            this threshold the match is considered a non-match.
        **kwargs: Optional
            fuzzy set specific parameters
        """
        super(FuzzyVocabularyMatcher, self).__init__(terms=vocabulary)
        self.matcher = FuzzySet(iterable=vocabulary,
                                gram_size_lower=kwargs.get('gram_size_lower', 2),
                                gram_size_upper=kwargs.get('gram_size_upper', 3),
                                use_levenshtein=kwargs.get('use_levenshtein', True),
                                rel_sim_cutoff=kwargs.get('rel_sim_cutoff', 1))
        self.best_matches_only = best_matches_only
        self.no_match_threshold = no_match_threshold
        # Maintain an internal cache for computed match results in  if the
        # cache_results flag is True.
        self._cache = dict() if cache_results else None

    def find_matches(self, query: str) -> List[StringMatchResult]:
        """Find matches for a given query string in the associated vocabulary.
        Depending on the implementation the result may contain more than one
        matched string from the vocabulary. Each match is a pair of matched
        values and match score.

        If no matches are found for a given query string the result is an empty
        list.

        Parameters
        ----------
        query: string
            Query string for which matches are returned.

        Returns
        -------
        list of tuples (string, float)
        """
        # Lookup results in the cache first.
        if self._cache and query in self._cache:
            return self._cache[query]
        # Compute list of all matches that satisfy the no-match threshold
        # constraint if the query string was not found in the cache.
        matches = list()
        results = self.matcher.get(query)
        if results is not None:
            for score, term in results:
                if score > self.no_match_threshold:
                    m = (term, score)
                    if self.best_matches_only and matches:
                        # If the best_matches_only flag is True we only need to add
                        # the match if the score is greater or equal than the
                        # current best score.
                        best_match = matches[0][1]
                        if score > best_match:
                            # Replace the list of matches with the current match
                            # since it has a higher score that the previous best
                            # match.
                            matches = [m]
                        elif score == best_match:
                            matches.append(m)
                    else:
                        matches.append(m)
            # Sort matches by decreasing score (only if best_matches_only is False
            # and we may have matches with different scores).
            if not self.best_matches_only:
                matches.sort(key=lambda m: m[1], reverse=True)
        # Add the result to the cache (if using).
        if self._cache is not None:
            self._cache[query] = matches
        return matches
