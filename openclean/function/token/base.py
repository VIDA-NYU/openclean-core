# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Interfaces for string tokenizer and token set transformers."""

from abc import ABCMeta, abstractmethod
from typing import Callable, Dict, List, Optional, Union

from openclean.data.types import Scalar, Value
from openclean.function.value.base import CallableWrapper, PreparedFunction, ValueFunction
from openclean.function.value.mapping import Standardize


# -- Mixin classes ------------------------------------------------------------

class StringTokenizer(metaclass=ABCMeta):
    """Interface for string tokenizer. A string tokenizer should be able to
    handle any scalar value (e.g., by first transforming numeric values into
    a string representation). The tokenizer returns a list of string values.
    """
    @abstractmethod
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
        raise NotImplementedError()  # pragma: no cover


class TokenTransformer(metaclass=ABCMeta):
    """The token transformer manipulates a list of string tokens. Manipulations
    may include removing tokens from an input list, rearranging tokens or even
    adding new tokens to the list. Defines a single transform method that takes
    a list of strings as input and returns a (modified) list of strings.
    """
    @abstractmethod
    def transform(self, tokens: List[str]) -> List[str]:
        """Transform a list of string tokens. Returns a modified copy of the
        input list of tokens.

        Patameters
        ----------
        tokens: list of string
            List of string tokens.

        Returns
        -------
        list of string
        """
        raise NotImplementedError()  # pragma: no cover


# -- Default tokenizer --------------------------------------------------------

class Tokens(PreparedFunction, StringTokenizer):
    """The default tokenizer is a simple wrapper around a given tokenizer and
    an (optional) token transformer that is applied on the output of the given
    tokenizer.

    This class provides to functionality to easily add default transformations
    to the generated token lists.

    The default tokenizer also extends the ValueFunction class to provide
    functionality to concatenate the generated token list to a token key string.
    """
    def __init__(
        self, tokenizer: StringTokenizer,
        transformer: Optional[Union[List[TokenTransformer], TokenTransformer]] = None,
        delim: Optional[str] = '', sort: Optional[bool] = False,
        reverse: Optional[bool] = False, unique: Optional[bool] = False
    ):
        """Initialize the tokenizer and optional token transformer. Provides the
        option to add basic transformations to the generated token lists.

        Parameters
        ----------
        tokenizer: openclean.function.token.base.StringTokenizer
            Tokenizer that is used to generate initial token list for given
            values.
        transformer: list or single object of openclean.function.token.base.TokenTransformer,
                default=None
            Optional transformer that is applied on generated token lists. This
            can also be a list of transformers that are applied sequentially.
        delim: string, default=''
            Delimiter that is used to concatenate tokens when used as a value
            function.
        sort: bool, default=False
            Return a sorted token list if True. Tokens are sorted in ascending
            order by default.
        reverse: bool, default=False
            Reverse token lists before returning them.
        unique: bool, default=True
            Remove duplicate tokens from the generated token lists.
        """
        self.tokenizer = tokenizer
        # Convert transformer to pipeline if it is of type list.
        if isinstance(transformer, list):
            transformer = TokenTransformerPipeline(transformers=transformer)
        self.transformer = transformer
        self.delim = delim
        # Add transformers for sorting, reverse, and unqiue if the respective
        # flags are set.
        postproc = list()
        if unique:
            postproc.append(UniqueTokens())
        if sort:
            postproc.append(SortTokens(reverse=reverse))
        elif reverse:
            postproc.append(ReverseTokens())
        if postproc:
            if self.transformer is not None:
                postproc = [self.transformer] + postproc
            self.transformer = TokenTransformerPipeline(transformers=postproc)

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
        return self.delim.join(self.tokens(value))

    def tokens(self, value: Scalar) -> List[str]:
        """Tokenize the given value using the associated tokenizer. Then modify
        the tokens with the optional token transformer.

        Parameters
        ----------
        value: scalar
            Value that is converted into a list of tokens.

        Returns
        -------
        list of string
        """
        tokens = self.tokenizer.tokens(value)
        if self.transformer is not None:
            tokens = self.transformer.transform(tokens)
        return tokens


# -- Basic token transformers -------------------------------------------------

class ReverseTokens(TokenTransformer):
    """Reverse a given list of string tokens."""
    def transform(self, tokens: List[str]) -> List[str]:
        """Return a reversed copy of the token list.

        Patameters
        ----------
        tokens: list of string
            List of string tokens.

        Returns
        -------
        list of string
        """
        return tokens[::-1]


class SortTokens(TokenTransformer):
    """Sort a given token list in ascending or descending order."""
    def __init__(self, key: Optional[Callable] = None, reverse: Optional[bool] = False):
        """Define the sort order and the optional sort key function.

        Parameters
        ----------
        key: callable, default=None
            Optional sort key function.
        reverse: bool, default=False
            Flag indicating whether to sort tokens in ascending (False) or
            descending (True) order.
        """
        self.sortkey = key
        self.reverse = reverse

    def transform(self, tokens: List[str]) -> List[str]:
        """Returns a sorted copy of the tken list.

        Patameters
        ----------
        tokens: list of string
            List of string tokens.

        Returns
        -------
        list of string
        """
        return sorted(tokens, key=self.sortkey, reverse=self.reverse)


class TokenPrefix(TokenTransformer):
    """Return a list that is a prefix for a given list. The returned list are
    a prefix for a given input of maximal length N (where N is a user-defined
    parameter). Input lists that have fewer elementes than N are returned as is.
    """
    def __init__(self, length: int):
        """Define the maximal length of the prefix.

        Parameters
        ----------
        length: int
            Maximum number of tokens in the returned lists.
        """
        self.length = length

    def transform(self, tokens: List[str]) -> List[str]:
        """Return a list that contains the first N elements of the input list,
        where N is the length parameter defined during initialization. If the
        input list does not have more than N elements the input is returned as
        it is.

        Patameters
        ----------
        tokens: list of string
            List of string tokens.

        Returns
        -------
        list of string
        """
        return tokens[:self.length] if len(tokens) > self.length else tokens


class TokenTransformerPipeline(TokenTransformer):
    """Sequnce of token transformers that are applied on a given input list of
    string tokens.
    """
    def __init__(self, transformers: List[TokenTransformer]):
        """Initialize the token transformers in the pipeline.

        Parameters
        ----------
        transformers: list of openclean.function.token.base.TokenTransformer
            List of token transformers that are applied in sequence on a given
            list of input tokens.
        """
        self.transformers = transformers

    def transform(self, tokens: List[str]) -> List[str]:
        """Transform a list of string tokens. Applies the transformers in the
        pipeline sequentially on the output of the respective successor in the
        pipeline.

        Patameters
        ----------
        tokens: list of string
            List of string tokens.

        Returns
        -------
        list of string
        """
        for transformer in self.transformers:
            tokens = transformer.transform(tokens)
        return tokens


class UniqueTokens(TokenTransformer):
    """Remove duplicate tokens to return a list of unique tokens."""
    def transform(self, tokens: List[str]) -> List[str]:
        """Returns a list of unique tokens from the input list.

        Patameters
        ----------
        tokens: list of string
            List of string tokens.

        Returns
        -------
        list of string
        """
        return list(set(tokens))


class UpdateTokens(TokenTransformer):
    """Update tokens by applying a value function to each of them."""
    def __init__(self, func: Union[Callable, ValueFunction]):
        """Initialize the update function for tokens.

        Parameters
        ----------
        func: callable or openclean.function.value.base.ValueFunction
            Function that is applied on input tokens to transform them.
        """
        # Ensure that the function is a value function.
        self.func = CallableWrapper(func) if not isinstance(func, ValueFunction) else func

    def transform(self, tokens: List[str]) -> List[str]:
        """Returns the list of tokens that results from applying the associated
        value function of each of the tokens in the input list.

        Patameters
        ----------
        tokens: list of string
            List of string tokens.

        Returns
        -------
        list of string
        """
        # Prepare function if necessary.
        f = self.func if self.func.is_prepared() else self.func.prepare(tokens)
        return f.apply(tokens)


# -- Shortcuts for common update functions ------------------------------------

class LowerTokens(UpdateTokens):
    """Convert all tokens in a given list to lower case."""
    def __init__(self):
        """Initialize the update function."""
        super(LowerTokens, self).__init__(func=CallableWrapper(func=str.lower))


class StandardizeTokens(UpdateTokens):
    """Standardize tokens in a given list using a stamdardization mapping."""
    def __init__(self, mapping: Union[Dict, Standardize]):
        """Initialize the standardization mapping.

        Parameters
        ----------
        mapping: dict or openclean.function.value.mapping.Standardize
            Standardization mapping.
        """
        # Ensure that the mapping is a standardization function.
        super(StandardizeTokens, self).__init__(
            func=Standardize(mapping) if isinstance(mapping, dict) else mapping
        )


class UpperTokens(UpdateTokens):
    """Convert all tokens in a given list to upper case."""
    def __init__(self):
        """Initialize the update function."""
        super(UpperTokens, self).__init__(func=CallableWrapper(func=str.upper))
