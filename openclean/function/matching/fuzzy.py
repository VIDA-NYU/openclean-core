# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Fuzzy Approximate String Matching"""

import re
import math
import operator
import collections

from typing import Iterable, List, Optional, Tuple, Union

from openclean.data.mapping import StringMatch
from openclean.function.matching.base import StringSimilarity
from openclean.function.similarity.text import LevenshteinDistance


class FuzzySimilarity(StringSimilarity):
    """FuzzySet implementation for the String Similarity class. This is a simple
    implementation that uses fuzzy string comparisons to do approximate string
    matching operations against the provided vocabulary.

    Note: it converts everything to lowercase and removes all punctuation except
    commas and spaces.
    """

    def __init__(
        self, vocabulary: Optional[Iterable[str]] = None,
        gram_size_lower: Optional[int] = 2, gram_size_upper: Optional[int] = 3,
        use_levenshtein: Optional[bool] = True, rel_sim_cutoff: Optional[float] = 1.
    ):
        """Initialize the associated vocabulary, and configuration parameters
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
        self.exact_set = {}
        self.match_dict = collections.defaultdict(list)
        self.items = {}
        self.use_levenshtein = use_levenshtein
        self.gram_size_lower = gram_size_lower
        self.gram_size_upper = gram_size_upper
        self.rel_sim_cutoff = rel_sim_cutoff
        for i in range(gram_size_lower, gram_size_upper + 1):
            self.items[i] = []

        vocabulary = [] if vocabulary is None else vocabulary
        for value in vocabulary:
            self.add(value)

    def add(self, value: str):
        """Create ngrams from a vocabulary word, calculate L2 norm and store values in
        in the internal dictionaries

        the steps are as such:
        - n grams are computed. e.g. tokyo -> -tokyo- (add start+end chars) -> -t, to, ok, ky, yo, o- (2grams)
        - 2 and 3 gram frequencies counted and norms calculated using it.
        - the norms along with words are stored in self.items for all words in the vocab
        - the ngrams along with frequencies are in self.match_dict
        - exact_set stores the lowercased entry:original in a dict and returns

        Parameters
        ----------
        value: str
            The vocabulary word to include
        """
        lvalue = value.lower()

        # if value is already added, do nothing
        if lvalue in self.exact_set:
            return False

        # create and maintain a dict of ngram frequencies for each added word
        for i in range(self.gram_size_lower, self.gram_size_upper + 1):
            items = self.items[i]
            idx = len(items)
            items.append(0)
            grams = gram_counter(lvalue, i)  # get the ngrams
            norm = math.sqrt(sum(x ** 2 for x in grams.values()))  # compute the L2 norm
            for gram, freq in grams.items():
                self.match_dict[gram].append((idx, freq))
            items[idx] = (norm, lvalue)
            self.exact_set[lvalue] = value

    def compute(self, value: str, gram_size: int) -> List[Tuple[float, str]]:
        """Computes the  ngrams from the query string and calculates distances with the vocabulary words
        to return matches with similarity greater than the threshold.

        Parameters
        ----------
        value: str
            the query string

        gram_size: int
            the n in n-gram

        Returns
        -------
            List[Tuple[float, str]]
        """
        lvalue = value.lower()
        matches = collections.defaultdict(float)
        grams = gram_counter(lvalue, gram_size)
        items = self.items[gram_size]
        norm = math.sqrt(sum(x**2 for x in grams.values()))

        # make a list of all the indices that have ngrams that match with ngrams of the word along with the
        # sum of all the products of the counts e.g. 6 possibilities for tokyok and hokokoko to match 'ok' (2x3)
        for gram, freq in grams.items():
            for idx, other_freq in self.match_dict.get(gram, ()):
                matches[idx] += freq * other_freq

        if not matches:
            return None

        # cosine similarity i.e. cos of the angle b/w the match possibility and the norm
        results = [(match_score / (norm * items[idx][0]), items[idx][1])
                   for idx, match_score in matches.items()]
        results.sort(reverse=True, key=operator.itemgetter(0))

        # if levenshtein preferred instead, for the top 50 results, calculate the levenshtein distance
        # b/w each match and query word
        if self.use_levenshtein:
            leven_distance = LevenshteinDistance()
            results = [(leven_distance(matched, lvalue), matched)
                       for _, matched in results[:50]]
            results.sort(reverse=True, key=operator.itemgetter(0))

        # return matches with similarity greater than the threshold
        score_threshold = results[0][0] * min(1.0, self.rel_sim_cutoff)
        return [(score, self.exact_set[lval]) for score, lval in results
                if score >= score_threshold]

    def search(self, key: str, default: Optional[Union[None, Tuple[float, str]]] = None) -> List[Tuple[float, str]]:
        """searches for the key for matches or returns the default value

        Parameters
        ----------
            key: str
                the query string
            default: Optional[None, Tuple[float, str]], default = None
                the default value to return if match not found
        """
        try:
            return self[key]
        except KeyError:
            return default

    def __getitem__(self, value):
        """Getter for dict. Queries the exact_set, if not found, calls the compute method and returns a list with
        tuples of matches: (score, matched_word)

        Parameters
        ----------
        value: str
            query string

        Returns
        -------
            List[Tuple[float, str]]

        Raises
        ------
            KeyError
        """
        lvalue = value.lower()
        result = self.exact_set.get(lvalue)  # look for an exact match first
        if result and self.rel_sim_cutoff >= 1:
            return [(1, result)]
        for i in range(self.gram_size_upper, self.gram_size_lower - 1, -1):  # if not found, start creating ngrams
            results = self.compute(value, i)
            if results is not None:
                return results
        raise KeyError(value)

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
            self.add(term)
        return [StringMatch(term=t, score=s) for s, t in self.search(query)]


# -- Utility methods ----------------------------------------------------------

def gram_counter(value: str, gram_size: int = 2) -> dict:
    """Counts the ngrams and their frequency from the given value

    Parameters
    ----------
    value: str
        The string to compute the n-grams from
    gram_size: int, default= 2
        The n in the n-gram

    Returns
    -------
        dict
    """
    result = collections.defaultdict(int)
    for value in gram_iterator(value, gram_size):
        result[value] += 1
    return result


def gram_iterator(value: str, gram_size: int = 2):
    """Iterates and yields all the ngrams from the given value

    Parameters
    ----------
    value: str
        The string to compute the n-grams from
    gram_size: int, default= 2
        The n in the n-gram
    """
    no_punc_regex = re.compile(r'[^\w, ]+')
    simplified = '-' + no_punc_regex.sub('', value.lower()) + '-'
    len_diff = gram_size - len(simplified)
    if len_diff > 0:
        simplified += '-' * len_diff
    for i in range(len(simplified) - gram_size + 1):
        yield simplified[i:i + gram_size]
