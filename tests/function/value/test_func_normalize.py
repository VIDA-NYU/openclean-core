# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for normalization functions."""

import pytest

from openclean.function.value.normalize import (
    divide_by_total, max_abs_scale, min_max_scale
)


def test_func_divide_by_total():
    """Test the divide by total normalization function."""
    f = divide_by_total(range(10))
    for v in range(10):
        assert f(v) == v / sum(range(10))
    # All zero
    f = divide_by_total([0, 0])
    assert f(0) == 0
    # Error for non-numeric values if the raise_error flag is not False
    with pytest.raises(ValueError):
        divide_by_total(list(range(10)) + ['A', 'B'])
    f = divide_by_total(list(range(10)) + ['A', 'B'], raise_error=False)
    for v in range(10):
        assert f(v) == v / sum(range(10))
    assert f('A') == 'A'
    assert f('B') == 'B'


def test_func_max_abs_scale():
    """Test the divide by absolute maximum normalization function."""
    f = max_abs_scale(range(10))
    for v in range(10):
        assert f(v) == v / 9
    # All zero
    f = max_abs_scale([0, 0])
    assert f(0) == 0
    # Error for non-numeric values if the raise_error flag is not False
    with pytest.raises(ValueError):
        max_abs_scale(list(range(10)) + ['A', 'B'])
    f = max_abs_scale(list(range(10)) + ['A', 'B'], raise_error=False)
    for v in range(10):
        assert f(v) == v / 9
    assert f('A') == 'A'
    assert f('B') == 'B'


def test_func_min_max_scale():
    """Test the min-max scale normalization function."""
    f = min_max_scale(range(1, 10))
    for v in range(10):
        assert f(v) == (v - 1) / 8
    # All zero
    f = min_max_scale([0, 0])
    assert f(0) == 0
    # Error for non-numeric values if the raise_error flag is not False
    with pytest.raises(ValueError):
        min_max_scale(list(range(10)) + ['A', 'B'])
    f = min_max_scale(list(range(10)) + ['A', 'B'], raise_error=False)
    for v in range(10):
        assert f(v) == v / 9
    assert f('A') == 'A'
    assert f('B') == 'B'
