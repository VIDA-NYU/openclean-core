# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions to normalize test values."""

from typing import Callable, Optional

import unicodedata

from openclean.data.types import Value
from openclean.function.value.base import PreparedFunction


"""Mapping of non-diacritic characters to their equivalent 'simple' characters.
This mapping is adopted from the FingerprintKeyer* in OpenRefine which in turn
is based (in part) on https://stackoverflow.com/a/1453284/167425 by Andreas Petersson.

* https://github.com/OpenRefine/OpenRefine/blob/master/main/src/com/google/refine/clustering/binning/FingerprintKeyer.java
"""
NONDIACRITICS = {
    '\u00DF': 'ss',  # sharp s
    '\u00E6': 'ae',
    '\u00F8': 'oe',
    '\u00A9': 'c',   # copyright character
    '\u00F0': 'd',   # Small letter Icelandic eth
    '\u0111': 'd',   # Small letter D with stroke
    '\u0256': 'd',   # Small letter African D
    '\u00FE': 'th',  # Lower case Icelandic thorn þ
    '\u01BF': 'w',   # Lower case Wynn from Old English modernly transliterated to w
    '\u0127': 'h',   # small H with stroke
    '\u0131': 'i',   # dotless I
    '\u0138': 'k',   # small letter Kra
    '\u0142': 'l',   # Bialystock
    '\u014B': 'n',   # Small letter Eng
    '\u017F': 's',   # long s
    '\u0167': 't',   # small letter T with stroke
    '\u0153': 'oe'
}

"""First characters of unicode character categories that are removed. Currently
we remove control characters 'C' and punctuation 'P'.
"""
REMOVE_CATEGORIES = {'C', 'P'}


class TextNormalizer(PreparedFunction):
    """Text normalizer that replaces non-diacritic characters, umlauts, accents,
    etc. with their equivalent ascii character(s).
    """
    def __init__(self, preproc: Optional[Callable] = None):
        """Initialize the optional pre-processing function. The pre-processor
        is applied to any input string as part of the normalization process.
        The pre-processor can execute transformations in addition to the
        normalization of unicode characters (e.g., transform characters to
        lower case).

        The pre-processor is a callable and not a ValueFunction because this
        class is a prepared value function that cannot prepare another value
        function.

        Parameters
        ----------
        preproc: callable, default=None
            Pre-processor that is applied to all input values. The default
            pre-processor will trim the value, convert all characters to lower
            case and replace (consecutive) whitespaces with a single blank
            space character.
        """
        self.preproc = preproc if preproc is not None else default_preproc

    def eval(self, value: Value) -> str:
        """Normalize a given value. Converts the value to string if it is not
        of type string. Then replaces all non-diacritic characters with their
        equivalent as defined in NONDIACRITICS. The last step is to use the
        uncide data normalize and encode function to replace umlauts, accents,
        etc. into their base character.

        Parameters
        ----------
        value: scalar or tuple
            Value from the list that was used to prepare the function.

        Returns
        -------
        string
        """
        # Apply pre-processing function to prepare the input value.
        value = self.preproc(value)
        # Replace punctuation, non-diacritic characters and control characters.
        # Assuming that punctuation is probably frequent, we always build a
        # list containing the sunstituted characters. We also maintain a flag
        # to indicate if any charaters where replaced. Only if that flag is True
        # at the end we will generate a new string.
        replace = list(value)
        has_replacements = False
        for i, c in enumerate(value):
            if c in NONDIACRITICS:
                replace[i] = NONDIACRITICS[c]
                has_replacements = True
            elif unicodedata.category(c)[0] in REMOVE_CATEGORIES:
                # Based on https://stackoverflow.com/questions/4324790
                replace[i] = ''
                has_replacements = True
        if has_replacements:
            value = ''.join(replace)
        # Normalize based on https://gist.github.com/j4mie/557354.
        return unicodedata\
            .normalize('NFKD', value)\
            .encode('ASCII', 'ignore')\
            .decode('utf-8')


# -- Basic pre-processing functions -------------------------------------------


def default_preproc(value: Value) -> str:
    """Default pre-processing for string normalization. Ensures that the given
    argument is a string. Removes leading and trailing whitespaces, converts
    characters to lower case, and replaces all (consecutive) whitespaces with a
    single blank space character.

    Parameters
    ----------
    value: scalar or tuple
        INput value that is being prepared for normalization.

    Returns
    -------
    string
    """
    # Ensure that the value is a string.
    value = str(value) if not isinstance(value, str) else value
    # Trim the value, convert to lower case, and replace multiple whitespaces
    # with a single blank space.
    return ' '.join(value.lower().split())
