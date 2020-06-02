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
    values = divide_by_total(range(10))
    maxval = sum(range(10))
    assert values == [v / maxval for v in range(10)]
    # All zero
    values = divide_by_total([0, 0])
    assert values == [0, 0]
    # Error for non-numeric values if the raise_error flag is not False
    with pytest.raises(ValueError):
        divide_by_total(list(range(10)) + ['A'])
    values = divide_by_total(list(range(10)) + ['A'], raise_error=False)
    assert values == [v / maxval for v in range(10)] + ['A']


def test_func_max_abs_scale():
    """Test the divide by absolute maximum normalization function."""
    values = max_abs_scale(range(10))
    assert values == [v / 9 for v in range(10)]
    # All zero
    values = max_abs_scale([0, 0])
    assert values == [0, 0]
    # Error for non-numeric values if the raise_error flag is not False
    with pytest.raises(ValueError):
        max_abs_scale(list(range(10)) + ['A', 'B'])
    values = max_abs_scale(list(range(10)) + ['A'], raise_error=False)
    assert values == [v / 9 for v in range(10)] + ['A']


def test_func_min_max_scale():
    """Test the min-max scale normalization function."""
    values = min_max_scale(range(1, 10))
    assert values == [(v - 1) / 8 for v in range(1, 10)]
    # All zero
    values = min_max_scale([0, 0])
    assert values == [0, 0]
    # Error for non-numeric values if the raise_error flag is not False
    with pytest.raises(ValueError):
        min_max_scale(list(range(10)) + ['A', 'B'])
    values = min_max_scale(list(range(1, 10)) + ['A'], raise_error=False)
    assert values == [(v - 1) / 8 for v in range(1, 10)] + ['A']
