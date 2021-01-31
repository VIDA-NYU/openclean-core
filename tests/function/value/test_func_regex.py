# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""unit tests for the single-value regular expression match operator."""

from openclean.function.value.regex import IsMatch, IsNotMatch


def test_func_match():
    """Test functionality of the match operator."""
    # -- IsMatch --------------------------------------------------------------
    f = IsMatch(r'\d+')
    assert f('123')
    assert f('123abc')
    assert f(123)
    assert not f('abc')
    # Full match
    f = IsMatch(r'\d+', fullmatch=True)
    assert f('123')
    assert not f('123abc')
    assert f(123)
    assert not f('abc')
    # Without type casting
    f = IsMatch(r'\d+', as_string=False)
    assert f('123')
    assert f('123abc')
    assert not f(123)
    assert not f('abc')
    # -- IsNotMatch -----------------------------------------------------------
    f = IsNotMatch(r'\d+')
    assert not f('123')
    assert not f('123abc')
    assert not f(123)
    assert f('abc')
    # Full match
    f = IsNotMatch(r'\d+', fullmatch=True)
    assert not f('123')
    assert f('123abc')
    assert not f(123)
    assert f('abc')
    # Without type casting
    f = IsNotMatch(r'\d+', as_string=False)
    assert not f('123')
    assert not f('123abc')
    assert f(123)
    assert f('abc')
