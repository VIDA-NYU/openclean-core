# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Sample consumer for data streams."""

import pytest

from openclean.operator.stream.collector import RowCount
from openclean.operator.stream.sample import SampleCollector


@pytest.mark.parametrize('rows,size', [(0, 0), (5, 10), (10, 10), (100, 10)])
def test_random_sample_generator(rows, size):
    """Test counting the number of rows in a generated sample."""
    consumer = SampleCollector(columns=[], n=size, consumer=RowCount())
    for i in range(rows):
        consumer.consume(i, [])
    assert consumer.close() == min(rows, size)
