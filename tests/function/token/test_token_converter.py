# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for token converter."""

import pytest

from openclean.function.token.base import Token
from openclean.function.token.convert import TokenListConverter, TokenMapper


def test_token_list_converter():
    """Test the token list converter."""
    m1 = TokenMapper(label='T', lookup={'a': 'b'})
    m2 = TokenMapper(label='S', lookup={'b': 'c'})
    f = TokenListConverter(converters=[m1, m2])
    # -- Empty token list -----------------------------------------------------
    assert f.transform([]) == []
    # -- Non-empty token list -------------------------------------------------
    tokens = f.transform([Token('a'), Token('b'), Token('c')])
    assert ''.join([t for t in tokens]) == 'bcc'


@pytest.mark.parametrize(
    'label,lookup,token,result_value',
    [
        ('T', {'a': 'b'}, Token('a', token_type='A'), 'b'),
        ('T', {'a', 'b'}, Token('a', token_type='A'), 'a')
    ]
)
def test_token_mapper(label, lookup, token, result_value):
    """Test functionality of the token mapper."""
    mapper = TokenMapper(label=label, lookup=lookup)
    assert mapper.contains(token)
    t = mapper.convert(token)
    assert t == result_value
    assert t.type() == label


def test_token_mapper_transform():
    """Test token transformer functionality of the token mapper."""
    f = TokenMapper(label='T', lookup={'a': 'b'})
    # -- Empty token list -----------------------------------------------------
    assert f.transform([]) == []
    # -- Non-empty token list -------------------------------------------------
    tokens = f.transform([Token('a'), Token('b', token_type='S')])
    assert ''.join([t for t in tokens]) == 'bb'
    assert ''.join([t.type() for t in tokens]) == 'TS'
