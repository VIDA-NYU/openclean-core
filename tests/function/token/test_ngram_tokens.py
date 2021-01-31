# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the n-gram string tokenizer."""

import pytest

from openclean.function.token.ngram import NGrams


@pytest.mark.parametrize(
    'value,result',
    [
        ('ab', ['ab']),
        ('abc', ['abc']),
        ('abcd', ['abc', 'bcd']),
        ('abcde', ['abc', 'bcd', 'cde'])
    ]
)
def test_ngrams_no_padding(value, result):
    """Test generating 3-grams with no additional padding."""
    f = NGrams(n=3)
    assert f.tokens(value) == result


@pytest.mark.parametrize(
    'value,pleft,pright,result',
    [
        ('ab', '$', None, ['$$a', '$ab']),
        ('ab', None, '#', ['ab#', 'b##']),
        ('ab', '$', '#', ['$$a', '$ab', 'ab#', 'b##']),
        ('abc', '$', '#', ['$$a', '$ab', 'abc', 'bc#', 'c##'])
    ]
)
def test_ngrams_with_padding(value, pleft, pright, result):
    """Test generating 3-grams with with additional padding."""
    f = NGrams(n=3, pleft=pleft, pright=pright)
    assert f.tokens(value) == result
