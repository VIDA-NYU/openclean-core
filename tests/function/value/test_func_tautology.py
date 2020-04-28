# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the tautology function."""

from openclean.function.value.tautology import tautology


def test_func_tautology():
    """Simple test for correct return values of the tautology function."""
    # Default return value is True
    f = tautology()
    assert f('A')
    assert f(1)
    assert f(False)
    # Use False as the default
    f = tautology(False)
    assert not f('A')
    assert not f(1)
    assert not f(False)
    # Use 1 as the default
    f = tautology(1)
    assert f('A') == 1
    assert f(1) == 1
    assert f(False) == 1
