# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for basic token list filters."""

import pytest

from openclean.function.token.base import Token
from openclean.function.token.filter import (
    FirstLastFilter, MinMaxFilter, RepeatedTokenFilter, TokenFilter,
    TokenTypeFilter
)
from openclean.function.value.base import UnpreparedFunction
from openclean.function.value.domain import IsInDomain


TOKENS = ['B', 'A', 'C']


def test_first_last_filter():
    """Test the first/last token filter."""
    assert FirstLastFilter().transform(TOKENS) == ['B', 'C']


def test_min_max_filter():
    """Test the min/max token filter."""
    assert MinMaxFilter().transform(TOKENS) == ['A', 'C']


def test_prepared_token_filter():
    """Test token filter with a prepared value function."""
    f = TokenFilter(predicate=IsInDomain(domain={'A', 'D'}))
    assert f.transform(TOKENS) == ['A']


@pytest.mark.parametrize(
    'values,result', [
        ([], []),
        (['a'], ['a']),
        (['a', 'a'], ['a']),
        (['a', 'b', 'a'], ['a', 'b', 'a']),
        (['a', 'b', 'b', 'a'], ['a', 'b', 'a']),
        (['a', 'a', 'b', 'a', 'a'], ['a', 'b', 'a'])
    ]
)
def test_repeated_token_filter(values, result):
    """Test the filter that removes repeated tokens."""
    tokens = [Token(v) for v in values]
    assert RepeatedTokenFilter().transform(tokens) == result


@pytest.mark.parametrize(
    'types,negated,result',
    [({'A'}, False, 'ac'), ({'A'}, True, 'bde'), ({'A', 'C'}, False, 'ace'), ({'A', 'C'}, True, 'bd')]
)
def test_token_type_filter(types, negated, result):
    """Test filtering tokens based on their token type."""
    filter = TokenTypeFilter(types=types, negated=negated)
    tokens = [
        Token('a', token_type='A'),
        Token('b', token_type='B'),
        Token('c', token_type='A'),
        Token('d', token_type='B'),
        Token('e', token_type='C'),
    ]
    assert ''.join(filter.transform(tokens)) == result


def test_unprepared_token_filter():
    """Test token filter with a value function that needs to be prepared."""

    class TestFunc(UnpreparedFunction):
        """Filter values base on the maximum of a given list."""
        _max = None

        def __call__(self, value):
            return value == self._max

        def prepare(self, values):
            self._max = max(values)
            return self

    f = TokenFilter(predicate=TestFunc())
    assert f.transform(TOKENS) == ['C']
