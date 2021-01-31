# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""String matcher that use phonetic algorithms for transforming the input
strings to normalized phonetic encodings before comparing them for being
equal using the exact matcher.
"""

from jellyfish import soundex, metaphone, nysiis
from typing import Callable

from openclean.function.matching.base import ExactSimilarity
from openclean.function.value.base import PreparedFunction


class PhoneticMatcher(ExactSimilarity, PreparedFunction):
    """String matcher for phonetic algorithms. Extends exact string similarity
    using a callable that implements a phonetic encoding algorithm.
    """
    def __init__(self, encoder: Callable):
        """Initialize the phonetic encoding function.

        Parameters
        ----------
        encoder: callable
            Function that is used to encode string values before comparing the
            returned encodings.
        """
        super(PhoneticMatcher, self).__init__(
            transformer=encoder,
            ignore_case=False
        )

    def eval(self, value: str) -> str:
        """The evaluation method for a phonetic matcher returns the encoding
        for a given value depending on the associated encoding algorithm.

        Parameters
        ----------
        value: string
            Value that is being encoded.

        Returns
        -------
        string
        """
        return self.transformer(value)


class Metaphone(PhoneticMatcher):
    """String matcher using the metaphone algorithm to encode strings. Uses the
    metaphone algorithm (included in the jellyfish library) to encode each
    string before comparing the codes.
    """
    def __init__(self):
        """Initialize the transformer in the super class."""
        super(Metaphone, self).__init__(encoder=metaphone)


class NYSIIS(PhoneticMatcher):
    """String matcher using the NYSIIS algorithm to encode strings. Uses the
    NYSIIS algorithm (developed by the New York State Identification and
    Intelligence System; included in the jellyfish library) to encode each
    string before comparing the codes.
    """
    def __init__(self):
        """Initialize the transformer in the super class."""
        super(NYSIIS, self).__init__(encoder=nysiis)


class Soundex(PhoneticMatcher):
    """String matcher using soundex encoding for a pair of strings. Uses the
    soundex function to encode each string to a four digit code before
    comparing the codes.
    """
    def __init__(self):
        """Initialize the transformer in the super class."""
        super(Soundex, self).__init__(encoder=soundex)
