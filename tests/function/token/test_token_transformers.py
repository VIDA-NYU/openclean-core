# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for basic token list transformers."""

from openclean.function.token.base import (
    ReverseTokens, SortTokens, TokenPrefix, TokenTransformerPipeline,
    UniqueTokens
)


def test_prefix_transformer():
    """Test the token prefix transformer."""
    tokens = ['B', 'A', 'C']
    assert TokenPrefix(length=2).transform(tokens) == ['B', 'A']
    assert TokenPrefix(length=3).transform(tokens) == tokens
    assert TokenPrefix(length=4).transform(tokens) == tokens


def test_reverse_transformer():
    """Test reversing a list of tokens."""
    assert ReverseTokens().transform(['A', 'B']) == ['B', 'A']


def test_sort_transformer():
    """Test sorting a list of tokens."""
    tokens = ['B', 'A', 'C']
    assert SortTokens().transform(tokens) == ['A', 'B', 'C']
    assert SortTokens(reverse=True).transform(tokens) == ['C', 'B', 'A']
    f = SortTokens(key=lambda x: x[1])
    assert f.transform(['A2', 'W3', 'Z1']) == ['Z1', 'A2', 'W3']
    f = SortTokens(key=lambda x: x[1], reverse=True)
    assert f.transform(['A2', 'W3', 'Z1']) == ['W3', 'A2', 'Z1']


def test_transformer_pipeline():
    """Test chaining token transformers."""
    f = TokenTransformerPipeline(transformers=[SortTokens(), TokenPrefix(length=2)])
    assert f.transform(['C', 'A', 'B']) == ['A', 'B']


def test_unique_transformer():
    """Test creating a list of unique tokens."""
    assert set(UniqueTokens().transform(['A', 'A', 'B'])) == {'A', 'B'}
