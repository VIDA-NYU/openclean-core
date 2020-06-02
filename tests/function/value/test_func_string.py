# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for string and character functions."""

import pytest

from openclean.function.value.string import (
    Capitalize, Lower, Split, Tokens, Upper
)


def test_func_string():
    """Test basic string manipulation functions."""
    # -- Lower ----------------------------------------------------------------
    f = Lower()
    assert f('ALICE') == 'alice'
    assert f(1) == 1
    # Convert numbers to string
    f = Lower(as_string=True)
    assert f('ALICE') == 'alice'
    assert f(1) == '1'
    # Raise errors for numeric values
    f = Lower(as_string=True, raise_error=True)
    assert f('ALICE') == 'alice'
    with pytest.raises(ValueError):
        f(1)
    f = Lower(as_string=False, raise_error=True)
    assert f('ALICE') == 'alice'
    with pytest.raises(ValueError):
        f(1)
    # -- Upper ----------------------------------------------------------------
    f = Upper()
    assert f('Alice') == 'ALICE'
    assert f(1) == 1
    f = Upper(raise_error=True)
    with pytest.raises(ValueError):
        f(1)
    # -- Capitalize -----------------------------------------------------------
    f = Capitalize()
    assert f('alice') == 'Alice'
    assert f(1) == 1
    f = Capitalize(raise_error=True)
    with pytest.raises(ValueError):
        f(1)
    # -- Split ----------------------------------------------------------------
    f = Split('1')
    assert f('213') == ['2', '3']
    f(123)
    f = Split('1', as_string=True)
    assert f('213') == ['2', '3']
    assert f(213) == ['2', '3']
    f = Split('1', as_string=True, raise_error=True)
    assert f('213') == ['2', '3']
    with pytest.raises(ValueError):
        f(123)


def test_string_consumer():
    """Test string functions with consumers."""
    f = Lower(lambda x: x.startswith('A'))
    assert not f('Alice')
    f = Lower(lambda x: x.startswith('a'))
    assert f('Alice')
    assert f.apply(['ABC', 'abc', 'def']) == [True, True, False]


def test_func_tokens():
    """Test string tokens predicate."""
    f = Tokens(',')
    assert f('1') == 1
    assert f('1,2') == 2
    assert f('1,2,3') == 3
    assert f(1) == 1
    f = Tokens('.')
    assert f(1.4) == 1
    f = Tokens('.', as_string=True)
    assert f(1.4) == 2
    f = Tokens('.', raise_error=True)
    with pytest.raises(ValueError):
        f(1.4)
