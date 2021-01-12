# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of token key functions. These functions are used to generate
keys for input values from intermediate token lists.
"""

from typing import Callable, Optional

from openclean.data.types import Value

from openclean.function.token.base import StringTokenizer
from openclean.function.token.split import Split
from openclean.function.value.base import PreparedFunction
from openclean.function.value.normalize.text import TextNormalizer


class Fingerprint(PreparedFunction):
    """Fingerprint key generator that is adopted from OpenRefine:
    http://github.com/OpenRefine/OpenRefine/blob/master/main/src/com/google/refine/clustering/binning/FingerprintKeyer.java

    The main difference here is that we allow the user to provide their custom
    tokenizer and normalization functions. The steps for creating the key are
    similar to those explaind in:
    https://github.com/OpenRefine/OpenRefine/wiki/Clustering-In-Depth

    1) remove leading and trailing whitespace
    2) convert string to lower case
    3) Normalize string by removing punctuation and control characters and
       replacing non-diacritic characters (if the default normalizer is used).
    4) Tokenize string by splitting on whitespace characters. Then sort the
       tokens and remove duplicates (if the default tokenizer is used).
    5) Concatenate remaining (sorted) tokens using a single space character as
       the delimiter.
    """
    def __init__(self, tokenizer: Optional[StringTokenizer] = None, normalizer: Optional[Callable] = None):
        """Initialize the tokenizer that is used by the fingerprint function
        and the optional normalizer. By default, a tokenizer is used that splits
        on whitespaces. the default normalizer is the openclean text normalizer.

        Note that the normalizer is a callable and not a ValueFunction. This
        is because the Fungerprint is a prepared value function and therefore
        could not prepare a value function.

        Patameters
        ----------
        tokenizer: openclean.function.token.base.StringTokenizer, default=None
            Tokenizer that is used during fingerprint generation.
        """
        self.tokenizer = tokenizer if tokenizer is not None else Split('\\s+', sort=True, unique=True)
        self.normalizer = normalizer if normalizer is not None else TextNormalizer()

    def eval(self, value: Value) -> str:
        """Tokenize a given value and return a concatenated string of the
        resulting tokens.

        Parameters
        ----------
        value: scalar or tuple
            Input value that is tokenized and concatenated.

        Returns
        -------
        string
        """
        # Ensure that the value is a string.
        value = str(value) if not isinstance(value, str) else value
        # Step 1-2) Trim the value and convert to lower case
        value = value.strip().lower()
        # Step 3) Normalize text value.
        value = self.normalizer(value)
        # Step 4-5) Tokenize the string. By default, the tokens are sorted and
        # duplicate tokens are removed. However, the use has the option to
        # override this behaviour by providing their custom tokenizer at object
        # instantiation.
        # The returned tokens are then concatenated using a single space.
        return ' '.join(self.tokenizer.tokens(value))
