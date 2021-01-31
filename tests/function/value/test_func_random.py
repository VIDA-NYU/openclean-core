# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for random value selector."""

import pytest

from openclean.function.value.random import RandomSelect


@pytest.mark.parametrize(
    'values,ignore_freq',
    [(list(range(10)), False), ([0] * 10 + [1] * 10 + list(range(3, 10)), True)]
)
def test_random_selection(values, ignore_freq):
    """Test random selection from a list of values."""
    # The sequence of selected values when selecting from a list with 10 elements
    # and a seed of 42 is 1, 0, 4, 3, ...
    r = RandomSelect(seed=42, ignore_freq=ignore_freq)
    # The returned constant function is expected to return 1 for all arguments.
    f = r.prepare(values)
    assert f(0) == 1
    assert f(1) == 1
    # The returned constant function is expected to return 1 for all arguments.
    f = r.prepare(values)
    assert f(0) == 0
    assert f(1) == 0
