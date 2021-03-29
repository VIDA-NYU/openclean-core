# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the string split tokenizer."""

import pytest

from openclean.function.token.split import ChartypeSplit, Split

import openclean.function.token.base as TT


@pytest.mark.parametrize(
    'value,result_tokens,result_types',
    [
        ('W35ST', ['W', '35', 'ST'], ['A', 'D', 'A']),
        ('W35ST/', ['W', '35', 'ST', '/'], ['A', 'D', 'A', TT.ANY]),
        ('W35ST/8AVE', ['W', '35', 'ST', '/', '8', 'AVE'], ['A', 'D', 'A', TT.ANY, 'D', 'A']),
        (1234, ['1234'], ['D']),
        (12.34, ['12', '.', '34'], ['D', TT.ANY, 'D']),
        ('WW', ['WW'], ['A']),
        ('', [], [])
    ]
)
def test_homogeneous_split(value, result_tokens, result_types):
    """Test tokenizer that splits on character types."""
    f = ChartypeSplit(chartypes=[(str.isalpha, 'A'), (str.isdigit, 'D')])
    tokens = f.tokens(value)
    assert tokens == result_tokens
    assert [t.type() for t in tokens] == result_types


def test_split_numeric_value():
    """Test tokenizer with numeric value."""
    assert Split(pattern=' ').tokens(12345) == ['12345']
    assert Split(pattern='1').tokens(214) == ['2', '4']


@pytest.mark.parametrize(
    'unique,sorted,reverse,result',
    [
        (False, False, False, ['A', 'C', 'A', 'B', 'D']),
        (False, False, True, ['D', 'B', 'A', 'C', 'A']),
        (False, True, False, ['A', 'A', 'B', 'C', 'D']),
        (False, True, True, ['D', 'C', 'B', 'A', 'A']),
        (True, True, False, ['A', 'B', 'C', 'D']),
        (True, True, True, ['D', 'C', 'B', 'A'])
    ]
)
def test_split_parameters(unique, sorted, reverse, result):
    """Test different transformation options for the returned token sets."""
    s = Split(pattern='\\s+', sort=sorted, reverse=reverse, unique=unique)
    assert s.tokens('A C \t A B  D') == result


@pytest.mark.parametrize(
    'value,result,unique',
    [
        ('WEST 35ST CO ST', ['35', 'CO', 'ST', 'ST', 'WEST'], False),
        ('WEST 35ST CO ST', ['35', 'CO', 'ST', 'WEST'], True)
    ]
)
def test_split_subtokens(value, result, unique):
    """Test nested tokenization using the subtokens option."""
    tokenizer = Split(
        pattern='\\s+',
        subtokens=ChartypeSplit(chartypes=[(str.isalpha, 'A'), (str.isdigit, 'D')]),
        unique=unique,
        sort=True
    )
    assert tokenizer.tokens(value) == result


def test_tokenize_column():
    """Test option to tokenize a list of values."""
    tokenizer = Split(pattern='\\s+', sort=True)
    tokens = tokenizer.encode(values=['W 35 ST', '5TH AVE'])
    assert len(tokens) == 2
    assert tokens[0] == ['35', 'ST', 'W']
    assert tokens[1] == ['5TH', 'AVE']
