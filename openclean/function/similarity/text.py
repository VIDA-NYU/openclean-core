# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of string similarity functions."""

from typing import Callable

import jellyfish

from openclean.function.similarity.base import SimilarityFunction


# -- Edit distance string similarity functions --------------------------------

class NormalizedEditDistance(SimilarityFunction):
    """String similarity function that is based on functions that compute an
    edit distance between a pair of strings.

    The similarity for a pair of strings based on edit distance is the defined
    as (1 - normalized distance).
    """
    def __init__(self, func: Callable):
        """Initialize the function that computes the edit distance between a
        pair of strings.

        Parameters
        ----------
        func: callable
            Functon that expects two strings are arguments.
        """
        self.func = func

    def sim(self, val_1: str, val_2: str) -> float:
        """Calculates the edit distance between two strings and returns the
        similarity between them as (1 - normalized distance). The normalized
        distance is the edit distance divided by the length of the longer of
        the two strings.

        Parameters
        ----------
        val_1: string
            Value 1
        val_2: string
            Value 2

        Returns
        -------
        float
        """
        edit_distance = self.func(val_1, val_2)
        return 1 - (float(edit_distance) / max(len(val_1), len(val_2)))


class DamerauLevenshteinDistance(NormalizedEditDistance):
    """String similarity function that is based on the Damerau-Levenshtein
    distance between two strings.
    """
    def __init__(self):
        """Initialize the edit distance function in the super class."""
        super(DamerauLevenshteinDistance, self).__init__(
            func=jellyfish.damerau_levenshtein_distance
        )


class HammingDistance(NormalizedEditDistance):
    """String similarity function that is based on the Hamming distance
    between two strings.
    """
    def __init__(self):
        """Initialize the edit distance function in the super class."""
        super(HammingDistance, self).__init__(func=jellyfish.hamming_distance)


class LevenshteinDistance(NormalizedEditDistance):
    """String similarity function that is based on the Levenshtein distance
    between two strings.
    """
    def __init__(self):
        """Initialize the edit distance function in the super class."""
        super(LevenshteinDistance, self).__init__(func=jellyfish.levenshtein_distance)


# -- String similarity functions ----------------------------------------------

class StringSimilarityFunction(SimilarityFunction):
    """Wrapper for existing string similarity functions that compute the
    similarity between a pair of strings as a float in the interval [0-1].
    """
    def __init__(self, func: Callable):
        """Initialize the function that computes similatiry between a
        pair of strings.

        Parameters
        ----------
        func: callable
            Functon that expects two strings are arguments.
        """
        self.func = func

    def sim(self, val_1: str, val_2: str) -> float:
        """Calculate the similarity beween the given pair of strings.

        Parameters
        ----------
        val_1: string
            Value 1
        val_2: string
            Value 2

        Returns
        -------
        float
        """
        return self.func(val_1, val_2)


class JaroSimilarity(StringSimilarityFunction):
    """String similarity function that is based on the Jaro similarity
    between two strings.
    """
    def __init__(self):
        """Initialize the edit distance function in the super class."""
        super(JaroSimilarity, self).__init__(func=jellyfish.jaro_similarity)


class JaroWinklerSimilarity(StringSimilarityFunction):
    """String similarity function that is based on the Jaro-Winkler distance
    between two strings.
    """
    def __init__(self):
        """Initialize the edit distance function in the super class."""
        super(JaroWinklerSimilarity, self).__init__(
            func=jellyfish.jaro_winkler_similarity
        )


# -- Match Rating Approach ----------------------------------------------------

class MatchRatingComparison(SimilarityFunction):
    """String similarity function that is based on the match rating algorithm
    that returns True if two strings are considered equivalent and False
    otherwise.

    To return a value in the interval of [0-1] a match rating result of True is
    translated to 1 and the result False is translated to 0.
    """
    def sim(self, val_1: str, val_2: str) -> float:
        """Use Match rating approach to compare the given strings.

        Returns 1 if the match rating algorithm coniders the given strings as
        equivalent and 0 otherwise.

        Parameters
        ----------
        val_1: string
            Value 1
        val_2: string
            Value 2

        Returns
        -------
        float
        """
        return 1 if jellyfish.match_rating_comparison(val_1, val_2) else 0
