# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functions to filter (remove) tokens from given token lists."""

from collections.abc import Container
from typing import List, Optional, Set

from openclean.function.token.base import SortTokens, Token, TokenTransformer, TokenTransformerPipeline
from openclean.function.value.base import ValueFunction


class FirstLastFilter(TokenTransformer):
    """Return a list that only contains the first and last element in a token
    list.
    """

    def transform(self, tokens: List[Token]) -> List[Token]:
        """Return a list that contains the first and last element from the input
        list. If the input is empty the result is empty as well.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
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


class RepeatedTokenFilter(TokenTransformer):
    """Remove consecutive identical tokens in a given sequence."""
    def transform(self, tokens: List[Token]) -> List[Token]:
        """Returns a list where no two consecutive tokens are identical.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
        """
        if len(tokens) < 2:
            return tokens
        # Create initial list containing the first token.
        prev = tokens[0]
        result = [prev]
        for i in range(1, len(tokens)):
            token = tokens[i]
            if token != prev:
                result.append(token)
            prev = token
        return result


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

    def transform(self, tokens: List[Token]) -> List[Token]:
        """Returns a list that contains only those tokens that satisfy the
        filter condition defined by the associated predicate.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
        """
        # Prepare the predicate if necessary.
        if not self.predicate.is_prepared():
            pred = self.predicate.prepare(tokens)
        else:
            pred = self.predicate
        # Return only those tokens that satisfy the predicate.
        return [t for t in tokens if pred(t)]


class TokenTypeFilter(TokenTransformer):
    """Filter tokens in a given list by their type."""
    def __init__(self, types: Set[str], negated: Optional[bool] = False):
        """Initialize the list of tpken types to filter on and the negated
        flag.

        If the negated flag is True, the filter will retain all tokens of types
        that do not occur in the given filter list.

        Parameters
        ----------
        types: set of string
            List of token types to filter on.
        negated: bool, default=False
            Determine whether to retain tokens of types that occur in the given
            set (*False*) or those of types that do not occur in the type set
            (*True*).
        """
        self.types = types
        self.negated = negated

    def transform(self, tokens: List[Token]) -> List[Token]:
        """Returns a list that contains only those tokens that satisfy the
        filter condition defined by the associated predicate.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
        """
        result = list()
        for t in tokens:
            is_in = not self.negated if t.type() in self.types else self.negated
            if is_in:
                result.append(t)
        return result
