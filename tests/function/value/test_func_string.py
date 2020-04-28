# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for string and character functions."""

import pytest

from openclean.function.value.character import first_char_in_range
from openclean.function.value.comp import geq
from openclean.function.value.string import (
    capitalize, lower, split, tokens, upper
)


def test_func_char():
    """Test basic functionality of the character-based functions."""
    f = first_char_in_range('A', 'H')
    assert f('Alice')
    assert not f('Zoe')
    assert not f('bob')
    assert not f(1)
    f = first_char_in_range('1', '5', as_string=True)
    assert f(1)
    # Error cases
    assert not f(None)
    f = first_char_in_range('A', 'H', as_string=True, ignore_empty=False)
    with pytest.raises(ValueError):
        f(None)


def test_func_string():
    """Test basic string manipulation functions."""
    # -- Lower ----------------------------------------------------------------
    f = lower()
    assert f('ALICE') == 'alice'
    assert f(1) == 1
    # Convert numbers to string
    f = lower(as_string=True)
    assert f('ALICE') == 'alice'
    assert f(1) == '1'
    # Raise errors for numeric values
    f = lower(as_string=True, raise_error=True)
    assert f('ALICE') == 'alice'
    with pytest.raises(ValueError):
        f(1)
    f = lower(as_string=False, raise_error=True)
    assert f('ALICE') == 'alice'
    with pytest.raises(ValueError):
        f(1)
    # -- Upper ----------------------------------------------------------------
    f = upper()
    assert f('Alice') == 'ALICE'
    assert f(1) == 1
    f = upper(raise_error=True)
    with pytest.raises(ValueError):
        f(1)
    # -- Capitalize -----------------------------------------------------------
    f = capitalize()
    assert f('alice') == 'Alice'
    assert f(1) == 1
    f = capitalize(raise_error=True)
    with pytest.raises(ValueError):
        f(1)
    # -- Split ----------------------------------------------------------------
    f = split('1')
    assert f('213') == ['2', '3']
    f(123)
    f = split('1', as_string=True)
    assert f('213') == ['2', '3']
    assert f(213) == ['2', '3']
    f = split('1', as_string=True, raise_error=True)
    assert f('213') == ['2', '3']
    with pytest.raises(ValueError):
        f(123)


def test_string_consumer():
    """Test string functions with consumers."""
    f = lower(first_char_in_range('A', 'Z'))
    assert not f('Alice')
    f = lower(first_char_in_range('a', 'z'))
    assert f('Alice')


def test_func_tokens():
    """Test string tokens predicate."""
    f = tokens(',', 2)
    assert not f('1')
    assert f('1,2')
    assert not f('1,2,3')
    assert not f(1)
    f = tokens(',', geq(2))
    assert not f('1')
    assert f('1,2')
    assert f('1,2,3')
    assert not f(1)
