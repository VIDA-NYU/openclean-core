# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for profiler statistics helper functions and classes."""

import pytest

from openclean.profiling.stats import MinMaxCollector


def test_init_error():
    """Test initialize with invalid arguments."""
    with pytest.raises(ValueError):
        MinMaxCollector(first_value=1, minmax=(1, 1))


@pytest.mark.parametrize(
    'init,minmax',
    [((1, 2), (1, 5)), ((0, 10), (0, 10))]
)
def test_multivalue_init(init, minmax):
    """Test the min,max collector when initialized with a pair of values."""
    # Initialize with a pair of values.
    consumer = MinMaxCollector(minmax=init)
    for i in range(1, 6):
        consumer.consume(i)
    assert (consumer.minimum, consumer.maximum) == minmax


@pytest.mark.parametrize(
    'init,minmax',
    [(0, (0, 5)), (2, (1, 5)), (10, (1, 10))]
)
def test_single_value_init(init, minmax):
    """Test the min,max collector when initialized with a single value."""
    # Initialize with a single value.
    consumer = MinMaxCollector(first_value=init)
    for i in range(1, 6):
        consumer.consume(i)
    assert (consumer.minimum, consumer.maximum) == minmax
