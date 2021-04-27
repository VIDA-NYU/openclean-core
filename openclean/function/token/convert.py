# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Converter for tokens that allows to change the value of a token and/or the
token type.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Set, Union

from openclean.function.token.base import Token, TokenTransformer


class TokenConverter(TokenTransformer, metaclass=ABCMeta):
    """Interface for token convertrs that change token values and/or token
    types. The converter interface consist of two methods: the ``contains``
    method checks whether the converter accepts a given token for conversion,
    and the ``convert`` method converts the token if it is accepted by he
    converter.
    """
    @abstractmethod
    def contains(self, token: Token) -> bool:
        """Test if the converter contains a conversion rule for the given
        token.

        Parameters
        ----------
        token: openclean.function.token.base.Token
            Token that is tested for acceptance by this converter.

        Returns
        -------
        bool
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def convert(self, token: Token) -> Token:
        """Convert the given token according to the conversion ruls that are
        implemented by the converter.

        Returns a modified token.

        Parameters
        ----------
        token: openclean.function.token.base.Token
            Token that is converted.

        Returns
        -------
        openclean.function.token.base.Token
        """
        raise NotImplementedError()  # pragma: no cover

    def transform(self, tokens: List[Token]) -> List[Token]:
        """Convert accpeted token in a given list of tokens.

        For each token in the given list, if the converter accepts the token it
        is transformed. Otherwise, the original token is added to the resulting
        token list.

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
            # Test if this converter accepts the token.
            if self.contains(t):
                # Transform the token.
                t = self.convert(t)
            result.append(t)
        return result


class TokenListConverter(TokenTransformer):
    """Converter for a list of tokens. Implements the token transformer mixin
    interface. Uses a list of converters to convert tokens i a given list. The
    first converter that accepts a token in the list is used to transform the
    token.
    """
    def __init__(self, converters: List[TokenConverter]):
        """Initialize the list of converters that are used to transform tokens.

        Parameters
        ----------
        converters: list of openclean.function.token.convert.TokenConverter
            List of converter that are used to transform tokens in a given
            list.
        """
        self.converters = converters

    def transform(self, tokens: List[Token]) -> List[Token]:
        """Transform a list of tokens.

        For each token in the given list, the initialized converters are used in
        given order. The first converter that accepts the token is used to
        convert it. If no converter accepts the token it is added to the result
        without changes.

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
            # Test each of the initialized converters in given order. The first
            # converter that accepts the token is used to transform it.
            for c in self.converters:
                if c.contains(t):
                    # Transform the token and stop the iteration.
                    t = c.convert(t)
                    break
            result.append(t)
        return result


class TokenMapper(TokenConverter):
    """Converter for tokens that uses a lookup table to map a given token to a
    new token value and a new token type. This class is used for example to
    standardize tokens for a semantic type.
    """
    def __init__(self, label: str, lookup: Union[Dict, Set]):
        """Initialize the token label and the lookup table.

        Parameters
        ----------
        label: string
            Token type label for converted tokens.
        lookup: dict or set
            Lookup table for tokens that are converted. If a set is given all
            tokens in the set are converted to the sme value but with the new
            token type label.
        """
        self.label = label
        self.lookup = {v: v for v in lookup} if isinstance(lookup, set) else lookup

    def contains(self, token: Token) -> bool:
        """Test if the given token is contained in the lookup table.

        Parameters
        ----------
        token: openclean.function.token.base.Token
            Token that is tested for acceptance by this converter.

        Returns
        -------
        bool
        """
        return token in self.lookup

    def convert(self, token: Token) -> Token:
        """Replace the given token with the respective value in the lookup table
        and the converter token type.

        Returns a modified token.

        Parameters
        ----------
        token: openclean.function.token.base.Token
            Token that is converted.

        Returns
        -------
        openclean.function.token.base.Token
        """
        return Token(self.lookup[token], token_type=self.label)
