# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the value list filter function."""

import pytest

from openclean.function.value.base import CallableWrapper
from openclean.function.value.filter import Filter


def is_positive(value):
    """Helper function that returns True if a given value is greater than
    zero.
    """
    return value > 0


def test_sequence_filter():
    """Test filter values in a sequence of values."""
    # Use a value function as argument.
    values = Filter(CallableWrapper(func=is_positive)).apply([1, -1, 2, 0, 3])
    assert values == [1, 2, 3]
    # Use a callable as argument.
    values = Filter(is_positive).apply([1, -1, 2, 0, 3])
    assert values == [1, 2, 3]
    # Test truth value parameter by inverting the result
    values = Filter(is_positive, truth_value=False).apply([1, -1, 2, 0, 3])
    assert values == [-1, 0]
    # Error case when passing a non-callable as function argument to Filter.
    with pytest.raises(TypeError):
        Filter('not a callable')
