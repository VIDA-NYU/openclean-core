# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions to filter (remove) tokens from given token lists."""

from typing import List

from openclean.function.token.base import SortTokens, TokenTransformer, TokenTransformerPipeline
from openclean.function.value.base import ValueFunction


class FirstLastFilter(TokenTransformer):
    """Return a list that only contains the first and last element in a token
    list.
    """

    def transform(self, tokens: List[str]) -> List[str]:
        """Return a list that contains the first and last element from the input
        list. If the input is empty the result is empty as well.

        Patameters
        ----------
        tokens: list of string
            List of string tokens.

        Returns
        -------
        list of string
        """
        return [tokens[0], tokens[-1]] if tokens else tokens


class MinMaxFilter(TokenTransformerPipeline):
    """Filter that returns the minimum and maximum token in a given list. This
    filter is implemented as a pipeline that first sorts the tokens and then
    returns the first and last token from the sorted list.
    """
    def __init__(self):
        """Initialize the transfromers for sorting and value extraction."""
        super(MinMaxFilter, self).__init__(
            transformers=[SortTokens(), FirstLastFilter()]
        )


class TokenFilter(TokenTransformer):
    """Filter tokens based on a given predicate."""
    def __init__(self, predicate: ValueFunction):
        """Initialize the predicate that is used to filter tokens.

        Parameters
        ----------
        predicate: openclean.function.value.base.ValueFunction
            Value function that is used as predicate to filter tokens.
        """
        self.predicate = predicate

    def transform(self, tokens: List[str]) -> List[str]:
        """Returns a list that contains only those tokens that satisfy the
        filter condition defined by the associated predicate.

        Patameters
        ----------
        tokens: list of string
            List of string tokens.

        Returns
        -------
        list of string
        """
        # Prepare the predicate if necessary.
        if not self.predicate.is_prepared():
            pred = self.predicate.prepare(tokens)
        else:
            pred = self.predicate
        # Return only those tokens that satisfy the predicate.
        return [t for t in tokens if pred(t)]
