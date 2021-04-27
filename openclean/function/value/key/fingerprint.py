# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of token key functions. These functions are used to generate
keys for input values from intermediate token lists. The classes resemble
similar functionality as found in OpenRefine:

https://github.com/OpenRefine/OpenRefine/wiki/Clustering-In-Depth
"""

from typing import Callable, Optional

from openclean.data.types import Value

from openclean.function.token.base import Tokenizer
from openclean.function.token.ngram import NGrams
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
    def __init__(self, tokenizer: Optional[Tokenizer] = None, normalizer: Optional[Callable] = None):
        """Initialize the tokenizer that is used by the fingerprint function
        and the optional normalizer. By default, a tokenizer is used that splits
        on whitespaces. the default normalizer is the openclean text normalizer.

        Note that the normalizer is a callable and not a ValueFunction. This
        is because the Fingerprint is a prepared value function and therefore
        could not prepare another value function.

        Parameters
        ----------
        tokenizer: openclean.function.token.base.Tokenizer, default=None
            Tokenizer that is used during fingerprint generation.
        normalizer: callable, default=None
            Callable that is used to normalize values before token generation.
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
        # Step 1-3) Normalize text value (i fdefault normalizer is used.
        value = self.normalizer(value)
        # Step 4-5) Tokenize the string. By default, the tokens are sorted and
        # duplicate tokens are removed. However, the user has the option to
        # override this behaviour by providing a custom tokenizer when the
        # Fingerpring object s instantiated. The tokens that are returned by
        # the tokenizer are concatenated using a single blank space.
        return ' '.join(self.tokenizer.tokens(value))


class NGramFingerprint(Fingerprint):
    """Fingerprint key generator that uses an n-gram tokenizer instead of the
    default tokenizer. This is a shortcut to instantiate the Fingerprint key
    generator.
    """
    def __init__(
        self, n: int, pleft: Optional[str] = None, pright: Optional[str] = None,
        normalizer: Optional[Callable] = None
    ):
        """Create an instance of the Fingerprint key generator that uses an
        n-gram tokenizer instead of the default tokenizer. Provides the option
        to set the n-gram tokenizer parameters.

        Parameters
        ----------
        n: int
            Length of generated n-grams.
        pleft: str, default=None
            Padding character that is used to create a left-padding for each
            processed value of length n-1.
        pright: str, default=None
            Padding character that is used to create a right-padding for each
            processed value of length n-1.
        normalizer: callable, default=None
            Callable that is used to normalize values before token generation.
        """
        super(NGramFingerprint, self).__init__(
            tokenizer=NGrams(n=n, pleft=pleft, pright=pright),
            normalizer=normalizer
        )
