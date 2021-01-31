# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
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
    assert is_empty(None)
    assert is_empty('')
    assert not is_empty('  ')
    assert not is_empty(1)
    # Ignore whitespace
    assert is_empty(None, ignore_whitespace=True)
    assert is_empty('', ignore_whitespace=True)
    assert is_empty('  ', ignore_whitespace=True)
    assert not is_empty(1, ignore_whitespace=True)
    # -- IsNotEmpty -----------------------------------------------------------
    assert not is_not_empty(None)
    assert not is_not_empty('')
    assert is_not_empty('  ')
    assert is_not_empty(1)
    # Ignore whitespace
    assert not is_not_empty(None, ignore_whitespace=True)
    assert not is_not_empty('', ignore_whitespace=True)
    assert not is_not_empty('  ', ignore_whitespace=True)
    assert is_not_empty(1, ignore_whitespace=True)


def test_func_is_none():
    """Test the is none operator."""
    # -- IsNone ---------------------------------------------------------------
    assert is_none(None)
    assert not is_none('')
    assert not is_none('  ')
    assert not is_none(1)
    # -- IsNotNone ------------------------------------------------------------
    assert not is_not_none(None)
    assert is_not_none('')
    assert is_not_none('  ')
    assert is_not_none(1)
