# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for single-value None and empty predicates."""

from openclean.function.value.null import (
    is_empty, is_not_empty, is_none, is_not_none
)


def test_func_is_empty():
    """Test the is empty operator."""
    # -- IsEmpty --------------------------------------------------------------
    f = is_empty()
    assert f(None)
    assert f('')
    assert not f('  ')
    assert not f(1)
    # Ignore whitespace
    f = is_empty(ignore_whitespace=True)
    assert f(None)
    assert f('')
    assert f('  ')
    assert not f(1)
    # -- IsNotEmpty -----------------------------------------------------------
    f = is_not_empty()
    assert not f(None)
    assert not f('')
    assert f('  ')
    assert f(1)
    # Ignore whitespace
    f = is_not_empty(ignore_whitespace=True)
    assert not f(None)
    assert not f('')
    assert not f('  ')
    assert f(1)


def test_func_is_none():
    """Test the is none operator."""
    # -- IsNone ---------------------------------------------------------------
    f = is_none()
    assert f(None)
    assert not f('')
    assert not f('  ')
    assert not f(1)
    # -- IsNotNone ------------------------------------------------------------
    f = is_not_none()
    assert not f(None)
    assert f('')
    assert f('  ')
    assert f(1)
