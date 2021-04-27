# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Interfaces for string tokenizer and token set transformers."""

from abc import ABCMeta, abstractmethod
from typing import Callable, Dict, List, Optional, Tuple, Union

from openclean.data.types import Scalar, Value
from openclean.function.value.base import CallableWrapper, PreparedFunction, ValueFunction
from openclean.function.value.mapping import Standardize


# Definition of default raw token data types.
ALPHA = 'ALPHA'
ALPHANUM = 'ALPHANUM'
ANY = 'ANY'
DIGIT = 'NUMERIC'
PUNCTUATION = 'PUNC'
SPACE = 'SPACE'


class Token(str):
    """Tokens are strings that have an optional (semantic) type label.

    The values for type labels are not constraint. It is good practice, to use
    all upper case values for token types. The default token type is 'ANY'.

    This implementation is based on:
    https://bytes.com/topic/python/answers/32098-my-experiences-subclassing-string

    The order of creation is that the __new__ method is called which returns
    the object then __init__ is called.
    """
    def __new__(
        cls, value: str, token_type: Optional[str] = None,
        rowidx: Optional[int] = None
    ):
        """Initialize the String object with the given value.

        the token type is ignored.

        Parameters
        ----------
        value: string
            Token value.
        token_type: string, default=None
            Unique token type identifier.
        """
        return str.__new__(cls, value)

    def __init__(
        self, value: str, token_type: Optional[str] = None,
        rowidx: Optional[int] = None
    ):
        """Initialize the token type identifier.

        The token value has already been initialized by the __new__ method that
        is called prior to the __init__ method.

        Parameters
        ----------
        value: string
            Token value.
        token_type: string, default=None
            Unique token type identifier.
        rowidx: int, default=None
            Optional identifier for the row that contained the value that this
            token was generated from.
        """
        self.token_type = token_type
        self.rowidx = rowidx

    @property
    def regex_type(self) -> str:
        """Synonym for getting the token type.

        Returns
        -------
        str
        """
        return self.type()

    @regex_type.setter
    def regex_type(self, token_type) -> str:
        """Set the token type.

        Parameters
        ----------
        token_type: string, default=None
            Unique token type identifier.
        """
        self.token_type = token_type

    @property
    def size(self) -> int:
        """Synonym to get the length of the token.

        Returns
        -------
        int
        """
        return len(self)

    def to_tuple(self) -> Tuple[str, str, int]:
        """Returns a tuple of the string, type and value size.

        Returns
        -------
        tuple of string, string, int
        """
        return tuple([self, self.token_type, len(self)])

    def type(self) -> str:
        """Get token type value.

        This is a wrapper around the ``token_type`` property. Returns the
        default token type 'ANY' if no type was given when the object was
        created.

        Returns
        -------
        string
        """
        return self.token_type if self.token_type is not None else ANY

    @property
    def value(self) -> str:
        """Get  the value for this token.

        Returns
        -------
        str
        """
        return str(self)


# -- Mixin classes ------------------------------------------------------------

class Tokenizer(metaclass=ABCMeta):
    """Interface for string tokenizer. A string tokenizer should be able to
    handle any scalar value (e.g., by first transforming numeric values into
    a string representation). The tokenizer returns a list of token objects.
    """
    def encode(self, values: List[Value]) -> List[List[Token]]:
        """Encodes all values in a given column (i.e., list of values) into
        their type representations and tokenizes each value.

        Parameters
        ----------
        values: list of scalar
            List of column values

        Returns
        -------
        list of list of openclean.function.token.base.Token
        """
        encoded = list()
        for rowidx, value in enumerate(values):
            encoded.append(self.tokens(rowidx=rowidx, value=value))
        return encoded

    @abstractmethod
    def tokens(self, value: Scalar, rowidx: Optional[int] = None) -> List[Token]:
        """Convert a given scalar values into a list of string tokens. If a
        given value cannot be converted into tokens None should be returned.

        The order of tokens in the returned list not necissarily corresponds to
        their order in the original value. This is implementation dependent.

        Parameters
        ----------
        value: scalar
            Value that is converted into a list of tokens.
        rowidx: int, default=None
            Optional index of the dataset row that the value originates from.

        Returns
        -------
        list of openclean.function.token.base.Token
        """
        raise NotImplementedError()  # pragma: no cover


class TokenTransformer(metaclass=ABCMeta):
    """The token transformer manipulates a list of string tokens. Manipulations
    may include removing tokens from an input list, rearranging tokens or even
    adding new tokens to the list. Defines a single transform method that takes
    a list of strings as input and returns a (modified) list of strings.
    """
    @abstractmethod
    def transform(self, tokens: List[Token]) -> List[Token]:
        """Transform a list of string tokens. Returns a modified copy of the
        input list of tokens.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
        """
        raise NotImplementedError()  # pragma: no cover


# -- Default tokenizer --------------------------------------------------------

class Tokens(PreparedFunction, Tokenizer):
    """The default tokenizer is a simple wrapper around a given tokenizer and
    an (optional) token transformer that is applied on the output of the given
    tokenizer.

    This class provides to functionality to easily add default transformations
    to the generated token lists.

    The default tokenizer also extends the ValueFunction class to provide
    functionality to concatenate the generated token list to a token key string.
    """
    def __init__(
        self, tokenizer: Tokenizer,
        transformer: Optional[Union[List[TokenTransformer], TokenTransformer]] = None,
        delim: Optional[str] = '', sort: Optional[bool] = False,
        reverse: Optional[bool] = False, unique: Optional[bool] = False
    ):
        """Initialize the tokenizer and optional token transformer. Provides the
        option to add basic transformations to the generated token lists.

        Parameters
        ----------
        tokenizer: openclean.function.token.base.Tokenizer
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

    def tokens(self, value: Scalar, rowidx: Optional[int] = None) -> List[Token]:
        """Tokenize the given value using the associated tokenizer. Then modify
        the tokens with the optional token transformer.

        Parameters
        ----------
        value: scalar
            Value that is converted into a list of tokens.
        rowidx: int, default=None
            Optional index of the dataset row that the value originates from.

        Returns
        -------
        list of openclean.function.token.base.Token
        """
        tokens = self.tokenizer.tokens(value=value, rowidx=rowidx)
        if self.transformer is not None:
            tokens = self.transformer.transform(tokens)
        return tokens


# -- Basic token transformers -------------------------------------------------

class ReverseTokens(TokenTransformer):
    """Reverse a given list of string tokens."""
    def transform(self, tokens: List[Token]) -> List[Token]:
        """Return a reversed copy of the token list.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
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

    def transform(self, tokens: List[Token]) -> List[Token]:
        """Returns a sorted copy of the tken list.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
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

    def transform(self, tokens: List[Token]) -> List[Token]:
        """Return a list that contains the first N elements of the input list,
        where N is the length parameter defined during initialization. If the
        input list does not have more than N elements the input is returned as
        it is.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
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

    def transform(self, tokens: List[Token]) -> List[Token]:
        """Transform a list of string tokens. Applies the transformers in the
        pipeline sequentially on the output of the respective successor in the
        pipeline.

        Parameters
        ----------
        tokens: list of string
            List of string openclean.function.token.base.Token.

        Returns
        -------
        list of openclean.function.token.base.Token
        """
        for transformer in self.transformers:
            tokens = transformer.transform(tokens)
        return tokens


class UniqueTokens(TokenTransformer):
    """Remove duplicate tokens to return a list of unique tokens."""
    def transform(self, tokens: List[Token]) -> List[Token]:
        """Returns a list of unique tokens from the input list.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
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

    def transform(self, tokens: List[Token]) -> List[Token]:
        """Returns the list of tokens that results from applying the associated
        value function of each of the tokens in the input list.

        Parameters
        ----------
        tokens: list of openclean.function.token.base.Token
            List of string tokens.

        Returns
        -------
        list of openclean.function.token.base.Token
        """
        # Prepare function if necessary.
        f = self.func if self.func.is_prepared() else self.func.prepare(tokens)
        return [Token(f(t), token_type=t.type()) for t in tokens]


class CapitalizeTokens(UpdateTokens):
    """Capitalize all tokens in a given list."""
    def __init__(self):
        """Initialize the update function."""
        super(CapitalizeTokens, self).__init__(func=CallableWrapper(func=str.capitalize))


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
