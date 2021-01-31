# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the string split tokenizer."""

import pytest

from openclean.function.token.split import ChartypeSplit, Split


def test_homogeneous_split():
    """Test tokenizer that splits on character types."""
    f = ChartypeSplit(chartypes=[str.isalpha, str.isdigit])
    assert f.tokens('W35ST') == ['W', '35', 'ST']
    assert f.tokens('W35ST/') == ['W', '35', 'ST', '/']
    assert f.tokens('W35ST/8AVE') == ['W', '35', 'ST', '/', '8', 'AVE']
    assert f.tokens(1234) == ['1234']
    assert f.tokens(12.34) == ['12', '.', '34']


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
