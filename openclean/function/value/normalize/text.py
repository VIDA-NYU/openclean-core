# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions to normalize test values."""

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
        # Ensure that the value is a string.
        value = str(value) if not isinstance(value, str) else value
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
