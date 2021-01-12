# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""String tokenizer that is a wrapper around the string split method."""

from typing import List, Optional

from openclean.data.types import Scalar
from openclean.function.token.base import StringTokenizer


class Split(StringTokenizer):
    """String tokenizer that is a wrapper around the `split()` method for
    strings. Defines a few extra parameters to modify the generated token
    lists.
    """
    def __init__(
        self, pattern: str, sort: Optional[bool] = False,
        reverse: Optional[bool] = False, unique: Optional[bool] = False
    ):
        """Initialize the tokenizer parameters.

        Parameters
        ----------
        pattern: string
            Regular expression defining the split criteria.
        sort: bool, default=False
            Return a sorted token list if True. Tokens are sorted in ascending
            order by default.
        reverse: bool, default=False
            Reverse token lists before returning them.
        unique: bool, default=True
            Remove duplicate tokens from the generated token lists.
        """
        self.pattern = pattern
        self.sort = sort
        self.reverse = reverse
        self.unique = unique

    def tokens(self, value: Scalar) -> List[str]:
        """Convert a given scalar values into a list of string tokens. If a
        given value cannot be converted into tokens None should be returned.

        The order of tokens in the returned list not necissarily corresponds to
        their order in the original value. This is implementation dependent.

        Parameters
        ----------
        value: scalar
            Value that is converted into a list of tokens.

        Returns
        -------
        list of string
        """
        # Convert value to string if necessary
        if not isinstance(value, str):
            value = str(value)
        # Use split and the defined pattern to generate initial token list.
        tokens = value.split(self.pattern)
        # Remove duplicates if the unique flag is True.
        if self.unique:
            tokens = list(set(tokens))
        # Sort tokens and reverse the token list if the respective flags are
        # set to True.
        if self.sort:
            # Can take care of sorting and reverse in one statement here.
            tokens = sorted(tokens, reverse=self.reverse)
        elif self.reverse:
            # Reverse list even if sort is False.
            tokens = tokens[::-1]
        return tokens
