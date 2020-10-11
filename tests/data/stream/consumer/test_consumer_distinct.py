# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Distinct consumer for data streams."""

from openclean.data.stream.consumer import Distinct


def test_distinct_consumer_ternary():
    """Test frequency counts for ternary rows."""
    consumer = Distinct()
    consumer.consume(1, ['A', 1])
    consumer.consume(2, ['A', 2])
    consumer.consume(3, ['B', 1])
    consumer.consume(4, ['B', 1])
    counts = consumer.close()
    assert len(counts) == 3
    assert counts[('A', 1)] == 1
    assert counts[('A', 2)] == 1
    assert counts[('B', 1)] == 2


def test_distinct_consumer_unary():
    """Test frequency counts for distinct values for unary rows."""
    consumer = Distinct()
    consumer.consume(1, [3])
    consumer.consume(2, [4])
    consumer.consume(3, [3])
    counts = consumer.close()
    assert len(counts) == 2
    assert counts[3] == 2
    assert counts[4] == 1
