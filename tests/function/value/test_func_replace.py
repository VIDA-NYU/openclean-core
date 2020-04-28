# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the mapping and replace functions."""

import pytest

from openclean.function.value.domain import is_in
from openclean.function.value.replace import lookup, replace, varreplace
from openclean.function.value.string import lower


def test_lookup_table():
    """Simple test of the lookup table function."""
    # Default behavior
    f = lookup({'A': 1, 'B': 2, 'C': 3})
    assert f('A') == 1
    assert f('B') == 2
    assert f('D') is None
    f = lookup({'A': 1, 'B': 2, 'C': 3}, default_value=-1)
    assert f('A') == 1
    assert f('D') == -1
    # Return self as default
    f = lookup({'A': 1, 'B': 2, 'C': 3}, default_value=-1, for_missing='self')
    assert f('A') == 1
    assert f('D') == 'D'
    # Error for missing keys
    f = lookup({'A': 1, 'B': 2, 'C': 3}, default_value=-1, for_missing='error')
    assert f('A') == 1
    with pytest.raises(KeyError):
        f('D')
    # Convert values to string
    f = lookup({'1': 'one', '2': 'two'})
    assert f(1) is None
    f = lookup({'1': 'one', '2': 'two'}, as_string=True)
    assert f(1) == 'one'


def test_lookup_table_for_missing():
    """Test different behavior of the lookup table for missing values."""
    f = lookup({('A', 1): 'A1', ('B', 2): 'B2'}, for_missing=1)
    assert f(('A', 1)) == 'A1'
    assert f(('A', 2)) == 2
    f = lookup({('A', 1): 'A1', ('B', 2): 'B2'}, for_missing=[1, 0])
    assert f(('A', 1)) == 'A1'
    assert f(('A', 2)) == (2, 'A')
    # Error cases
    with pytest.raises(ValueError):
        lookup({'A': 1}, for_missing=1.4)
    with pytest.raises(ValueError):
        lookup({'A': 1}, for_missing='null')
    with pytest.raises(ValueError):
        lookup({'A': 1}, for_missing=['1'])


def test_replace_single_argument():
    """Test replace function for single values."""
    values = ['A', 'B']
    f = replace(is_in(values), 0)
    assert f('A') == 0
    assert f('B') == 0
    assert f('C') == 'C'
    # Replace with modificatin function
    values = ['A', 'B']
    f = replace(is_in(values), lower)
    assert f('A') == 'a'
    assert f('B') == 'b'
    assert f('C') == 'C'


def test_replace_variable_arguments():
    """Test replace function for variable argument lists."""
    def both_equal(v1, v2):
        return v1 == v2
    f = varreplace(both_equal, (0, 0))
    assert f('A', 'A') == (0, 0)
    assert f('A', 'B') == ('A', 'B')
