# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes and types for string matching functions."""

from abc import ABCMeta, abstractmethod
from typing import Callable, Iterable, List, Optional

from openclean.data.mapping import Mapping, ExactMatch, NoMatch, StringMatch
from openclean.function.value.text import to_lower
from openclean.util.core import scalar_pass_through


# -- String similarity --------------------------------------------------------

class StringSimilarity(metaclass=ABCMeta):
    """Abstract base class for functions that compute similarity scores between
    a list of terms and a query string and return a list of StringMatch results.
    String similarity scores should be values in the interval [0-1] where 0
    indicates no match and 1 indicates an exact match.
    """
    @abstractmethod
    def match(self, vocabulary: Iterable[str], query: str) -> List[StringMatch]:
        """Compute a similarity score for a string against items from a vocabulary
        iterable. A score of 1 indicates an exact match. A score of 0 indicates a
        no match.

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
        raise NotImplementedError()  # pragma: no cover

    def score(self, vocabulary: Iterable[str], query: str) -> List[StringMatch]:
        """Synonym for the match function. Compute a similarity score for a string
        against items from a vocabulary iterable. A score of 1 indicates an exact
        match. A score of 0 indicates a no match.

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
        return self.match(vocabulary, query)


class ExactSimilarity(StringSimilarity):
    """Implementation of the string similarity class that performs exact matches
    for string arguments. Allows to transform values before comparing them
    using a simple callable function that expects a single argument.

    The returned score is one for identical string and 0 for non-identical
    strings. The ignore_case flag allows to compare two strings ignoring their
    case.
    """
    def __init__(
        self,
        transformer: Optional[Callable] = scalar_pass_through,
        ignore_case: Optional[bool] = False
    ):
        """Set the value transformer and the ignore case flag for the matcher.
        If the ignore_case flag is True comapred values are converted to lower
        case before comparing the values (i.e., the transformer results) for
        equality.

        Parameters
        ----------
        transformer: callable, default=scalar_pass_through
            Transform values using the given function before comparing the
            results for equality.
        ignore_case: bool, default=False
            Convert compared values to lower case before comparison if this
            flag is True.
        """
        self.transformer = transformer
        self.ignore_case = ignore_case

    def match(self, vocabulary: Iterable[str], query: str) -> List[StringMatch]:
        """Cross reference query with the vocabulary strings for equality. Returns
        an exact match if the given arguments are the same and a NoMatch otherwise.

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
        matches = list()
        for term in vocabulary:
            l_term = to_lower(term) if self.ignore_case else term
            l_query = to_lower(query) if self.ignore_case else query
            m = ExactMatch(term) if self.transformer(l_term) == self.transformer(l_query) else NoMatch(term)
            matches.append(m)
        return matches


# -- Similarity-based vocabulary lookups --------------------------------------

class StringMatcher(metaclass=ABCMeta):
    """Abstract base class for functions that find matches for a query string
    in a given vocabulary (iterable of strings). Instances of this class are
    associated with a vocabulary. They return one or more matches from that
    vocabulary for a given query string.
    """
    def __init__(self, terms: Iterable[str]):
        """Initialize the terms in the vocabulary.

        Parameters
        ----------
        terms: iterable of string
            List of terms in the vocabulary.
        """
        self.vocabulary = terms

    @abstractmethod
    def find_matches(self, query: str) -> List[StringMatch]:
        """Find matches for a given query string in the associated vocabulary.
        Depending on the implementation the result may contain more than one
        matched string from the vocabulary. Each match is a pair of matched
        values and match score.

        Matches are sorted by decreasing similarity score. If no matches are
        found for a given query string the result is an empty list.

        Parameters
        ----------
        query: string
            Query string for which matches are returned.

        Returns
        -------
        list of (string, float) pairs
        """
        raise NotImplementedError()  # pragma: no cover

    def matched_values(self, query: str) -> List[str]:
        """Get only a list of matched values for a given query string. Excludes
        information about the match scores.

        Parameters
        ----------
        query: string
            Query string for which matches are returned.

        Returns
        -------
        list of string
        """
        return [m.term for m in self.find_matches(query)]


class DefaultStringMatcher(StringMatcher):
    """Default implementation for the string matcher. This is a simple
    implementation that naively computes the similarity between a query string
    and every string in the associated vocabulary by letting the string similarity
    object deal with the vocabulary directly.

    The default matcher allows the user to control the list of returned matches
    via two configuration parameters:

    - best_matches_only: If this flag is True only those matches that have the
      highest score will be returned in the result. If the flag is True all
      matches with a score greater than 0 (or the no_match_threshold, see
      below) are returned.
    - no_match_threshold: Score threshold that controls when a similarity score
      is considered a non-match.

    By default, the vocabulary matcher caches the results for found matches to
    avoid computing matches for the same query value twice. Caching can be
    disabled using the cache_results flag.
    """
    def __init__(
        self,
        vocabulary: Iterable[str],
        similarity: StringSimilarity,
        best_matches_only: Optional[bool] = True,
        no_match_threshold: Optional[float] = 0.,
        cache_results: Optional[bool] = True
    ):
        """Initialize the associated vocabulary, the similarity function, and
        the configuration parameters.

        Parameters
        ----------
        vocabulary: iterable of string
            List of terms in the associated vocabulary agains which query
            strings are matched.
        similarity: openclean.function.matching.base.StringSimilarity
            String similarity function that is used to compute scores between
            a query string and the values in the vocabulary.
        best_matches_only: bool, default=False
            If True, only matches with the highest score are returned.
        no_match_threshold: float, default=0.
            If the similarity score for a match with a query string is below
            this threshold the match is considered a non-match.
        cache_results: bool, default=True
            Keep an internal cache of match results to avoid computing matches
            for the same query value twice.
        """
        super(DefaultStringMatcher, self).__init__(terms=vocabulary)
        self.similarity = similarity
        self.best_matches_only = best_matches_only
        self.no_match_threshold = no_match_threshold
        # Maintain an internal cache for computed match results in  if the
        # cache_results flag is True.
        self._cache = dict() if cache_results else None

    def find_matches(self, query: str) -> List[StringMatch]:
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
        list of openclean.data.mapping.StringMatch
        """
        # Lookup results in the cache first.
        if self._cache and query in self._cache:
            return self._cache[query]
        # Compute list of all matches that satisfy the no-match threshold
        # constraint if the query string was not found in the cache.
        matches = list()
        results = self.similarity.score(self.vocabulary, query)
        if results is not None:
            for match in results:
                if match.score > self.no_match_threshold:
                    if self.best_matches_only and matches:
                        # If the best_matches_only flag is True we only need to add
                        # the match if the score is greater or equal than the
                        # current best score.
                        best_match = matches[0].score
                        if match.score > best_match:
                            # Replace the list of matches with the current match
                            # since it has a higher score that the previous best
                            # match.
                            matches = [match]
                        elif match.score == best_match:
                            matches.append(match)
                    else:
                        matches.append(match)
            # Sort matches by decreasing score (only if best_matches_only is False
            # and we may have matches with different scores).
            if not self.best_matches_only:
                matches.sort(key=lambda m: m.score, reverse=True)
        # Add the result to the cache (if using).
        if self._cache is not None:
            self._cache[query] = matches
        return matches


# -- Best match finder function -----------------------------------------------

def best_matches(
        values: Iterable[str], matcher: StringMatcher,
        include_vocab: Optional[bool] = False
) -> Mapping:
    """Generate a mapping of best matches for a list of values. For each value
    in the given list the best matches with a given vocabulary are computed and
    added to the returned mapping.

    If the include_vocab flag is False the resulting mapping will contain a
    mapping only for those values in the input list that do not already occur
    in the vocabulary, i.e., the unknown values with respect to the known
    vocabulary.

    Parameters
    ----------
    values: iterable of strings
        List of terms (e.g., from a data frame column) for which matches are
        computed for the returned mapping.
    matcher: openclean.function.matching.base.StringMatcher
        Matcher to compute matches for the terms in a controlled vocabulary.
    include_vocab: bool, default=False
        If this flag is False the resulting mapping will only contain matches
        for terms that are not in the vocabulary that is associated with the
        given similarity.

    Returns
    -------
    openclean.data.mapping.Mapping
    """
    map = Mapping()
    for val in values:
        if include_vocab or val not in matcher.vocabulary:
            map.add(val, matcher.find_matches(val))
    return map
