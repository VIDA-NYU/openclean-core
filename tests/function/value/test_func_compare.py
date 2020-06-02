# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for single-value compare operators."""

import pytest

from openclean.function.value.comp import (
    Eq, EqIgnoreCase, Geq, Gt, Leq, Lt, Neq
)
from openclean.function.value.datatype import to_int, to_float


def test_compare_eq():
    """Test the Eq compare operator."""
    # -- Eq -------------------------------------------------------------------
    f = Eq(1)
    assert f(1)
    assert not f(2)
    assert not f('1')
    f = Eq('abc')
    assert f('abc')
    assert not f('Abc')
    # Type error is not raised for == comparison between scalars
    f = Eq(1, raise_error=True)
    assert f(1)
    assert not f('1')
    # Test with type casting
    as_int = to_int
    f = Eq(1)
    assert not f('1')
    assert f(as_int('1'))
    as_float = to_float
    f = Eq(1.2)
    assert not f('1.2')
    assert f(as_float('1.2'))
    # -- EqIgnoreCase ---------------------------------------------------------
    f = EqIgnoreCase('abc')
    assert f('abc')
    assert f('Abc')
    assert not f(1)
    f = EqIgnoreCase('abc', raise_error=True)
    assert f('abc')
    assert f('Abc')
    assert not f(1)
    # -- Neq ------------------------------------------------------------------
    f = Neq(1)
    assert f(2)
    assert f('1')
    assert f('abc')
    # Ignore case
    f = Neq('abc')
    assert f('ABC')
    assert not f('abc')
    f = Neq('abc', ignore_case=True)
    assert not f('ABC')
    assert not f('abc')


def test_compare_greater():
    """Test greater than and greater or equal operators."""
    # -- Gt -------------------------------------------------------------------
    f = Gt(10)
    assert not f(9)
    assert not f(10)
    assert f(11)
    assert not f('abc')
    # Type error
    f = Gt(10, raise_error=True)
    assert f(11)
    with pytest.raises(TypeError):
        assert not f('abc')
    # -- Geq ------------------------------------------------------------------
    f = Geq(10)
    assert not f(9)
    assert f(10)
    assert f(11)
    assert not f('abc')
    # Type error
    f = Geq(10, raise_error=True)
    assert f(11)
    with pytest.raises(TypeError):
        assert not f('abc')


def test_compare_lower():
    """Test lower than and lower or equal operators."""
    # -- Lt -------------------------------------------------------------------
    f = Lt(10)
    assert f(9)
    assert not f(10)
    assert not f(11)
    assert not f('9')
    # Type error
    f = Lt(10, raise_error=True)
    assert f(9)
    with pytest.raises(TypeError):
        assert not f('abc')
    # -- Leq ------------------------------------------------------------------
    f = Leq(10)
    assert f(9)
    assert f(10)
    assert not f(11)
    assert not f('9')
    # Type error
    f = Leq(10, raise_error=True)
    assert f(10)
    with pytest.raises(TypeError):
        assert not f('abc')
