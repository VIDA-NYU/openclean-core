# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the base token class."""

from openclean.function.token.base import Token


def test_token_size():
    """Test the token size method."""
    assert Token('a').size == 1
    assert Token('abc').size == 3


def test_token_to_tuple():
    """Test the token to tuple function."""
    t = Token(value='abc', token_type='b', rowidx=1)
    assert t.to_tuple() == ('abc', 'b', 3)


def test_token_type():
    """Test token type values."""
    t = Token(value='a', token_type='b')
    assert t.value == 'a'
    assert t.type() == 'b'
    assert t.regex_type == 'b'
    t.regex_type = 'c'
    assert t.type() == 'c'
    assert t.regex_type == 'c'
