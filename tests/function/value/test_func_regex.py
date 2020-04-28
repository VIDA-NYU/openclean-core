# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""unit tests for the single-value regular expression match operator."""

from openclean.function.value.regex import is_match, is_not_match


def test_func_match():
    """Test functionality of the match operator."""
    # -- IsMatch --------------------------------------------------------------
    f = is_match(r'\d+')
    assert f('123')
    assert f('123abc')
    assert f(123)
    assert not f('abc')
    # Full match
    f = is_match(r'\d+', fullmatch=True)
    assert f('123')
    assert not f('123abc')
    assert f(123)
    assert not f('abc')
    # Without type casting
    f = is_match(r'\d+', as_string=False)
    assert f('123')
    assert f('123abc')
    assert not f(123)
    assert not f('abc')
    # -- IsNotMatch -----------------------------------------------------------
    f = is_not_match(r'\d+')
    assert not f('123')
    assert not f('123abc')
    assert not f(123)
    assert f('abc')
    # Full match
    f = is_not_match(r'\d+', fullmatch=True)
    assert not f('123')
    assert f('123abc')
    assert not f(123)
    assert f('abc')
    # Without type casting
    f = is_not_match(r'\d+', as_string=False)
    assert not f('123')
    assert not f('123abc')
    assert f(123)
    assert f('abc')
