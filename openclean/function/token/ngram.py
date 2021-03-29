# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""String tokenizer that returns a list of n-grams. A n-gram in this case is a
substring of length n.
"""

from typing import List, Optional

from openclean.data.types import Scalar
from openclean.function.token.base import Tokenizer, Token


class NGrams(Tokenizer):
    """Split values into lists of n-grams. n-grams are substrings of length n.
    Provides the option to pad stings with special characters to the left and
    right before computing n-grams. That is, if a left (right) padding character
    is given (e.g. $),  a string containing n-1 padding characters will be added
    to the left (right) of a given string before n-gams are computer.

    If no padding is specified (default) the value is split into n-grams as is.
    If the string does not contain more than n characters the string is returned
    as is.
    """
    def __init__(self, n: int, pleft: Optional[str] = None, pright: Optional[str] = None):
        """Initialize the length of the generated n-grams and the optional
        padding characters.

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
        """
        self.n = n
        self.pleft = pleft
        self.pright = pright

    def tokens(self, value: Scalar, rowidx: Optional[int] = None) -> List[Token]:
        """Convert a given scalar values into a list of n-grams. If the value
        length is not greater than n and no padding was specified, the returned
        list will only contain the given value.

        Parameters
        ----------
        value: scalar
            Value that is converted into a list of n-grams.
        rowidx: int, default=None
            Optional index of the dataset row that the value originates from.

        Returns
        -------
        list of openclean.function.token.base.Token
        """
        # Add left and right padding if specified.
        if self.pleft:
            value = self.pleft * (self.n - 1) + value
        if self.pright:
            value = value + self.pright * (self.n - 1)
        # If value length is not greater than n return single item list.
        if len(value) <= self.n:
            return [value]
        # Split value into n-grams.
        result = list()
        for i in range(len(value) - (self.n - 1)):
            result.append(Token(value=value[i: i + self.n], rowidx=rowidx))
        return result
