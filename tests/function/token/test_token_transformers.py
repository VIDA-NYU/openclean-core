# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for basic token list transformers."""

import pytest


from openclean.function.token.base import (
    CapitalizeTokens, LowerTokens, ReverseTokens, SortTokens, StandardizeTokens,
    Token, TokenPrefix, TokenTransformerPipeline, UniqueTokens, UpperTokens
)


def test_prefix_transformer():
    """Test the token prefix transformer."""
    values = ['B', 'A', 'C']
    tokens = [Token(t, token_type=t) for t in values]
    for i in [2, 3, 4]:
        result = concat_tokens(TokenPrefix(length=i).transform(tokens))
        assert result == ['{}{}'.format(v, v) for v in values[:min(i, 3)]]


def test_reverse_transformer():
    """Test reversing a list of tokens."""
    values = ['a', 'b']
    tokens = [Token(t, token_type=t.upper()) for t in values]
    result = concat_tokens(ReverseTokens().transform(tokens))
    assert result == ['bB', 'aA']


def test_sort_transformer():
    """Test sorting a list of tokens."""
    tokens = [Token(t, token_type=t.upper()) for t in ['b', 'a', 'c']]
    assert concat_tokens(SortTokens().transform(tokens)) == ['aA', 'bB', 'cC']
    assert concat_tokens(SortTokens(reverse=True).transform(tokens)) == ['cC', 'bB', 'aA']
    f = SortTokens(key=lambda x: x[1])
    assert f.transform([Token(t) for t in ['A2', 'W3', 'Z1']]) == ['Z1', 'A2', 'W3']
    f = SortTokens(key=lambda x: x[1], reverse=True)
    assert f.transform([Token(t) for t in ['A2', 'W3', 'Z1']]) == ['W3', 'A2', 'Z1']


def test_standardize_tokens():
    """Test token stadardization using a mapping dictionary."""
    f = StandardizeTokens({'ST': 'STREET'})
    assert f.transform([Token(t) for t in ['ROAD', 'ST', 'STREET']]) == ['ROAD', 'STREET', 'STREET']


@pytest.mark.parametrize(
    'func,result',
    [
        (CapitalizeTokens(), ['Ab', 'Bb', 'Cb']),
        (LowerTokens(), ['ab', 'bb', 'cb']),
        (UpperTokens(), ['AB', 'BB', 'CB'])
    ]
)
def test_token_case(func, result):
    """Test transformers that change token case."""
    tokens = func.transform([Token(t, token_type=t.upper()) for t in ['Ab', 'bb', 'CB']])
    tokens == result
    assert [t.type() for t in tokens] == ['AB', 'BB', 'CB']


def test_transformer_pipeline():
    """Test chaining token transformers."""
    f = TokenTransformerPipeline(transformers=[SortTokens(), TokenPrefix(length=2)])
    assert f.transform([Token(t) for t in ['C', 'A', 'B']]) == ['A', 'B']


def test_unique_transformer():
    """Test creating a list of unique tokens."""
    assert set(UniqueTokens().transform([Token(t) for t in ['A', 'A', 'B']])) == {'A', 'B'}


# -- Helper functions ---------------------------------------------------------

def concat_tokens(tokens):
    return ['{}{}'.format(t, t.type()) for t in tokens]
