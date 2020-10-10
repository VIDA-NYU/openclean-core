# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Filter consumer for data streams."""

from openclean.data.stream.consumer import Collector, Filter
from openclean.function.eval.base import Col


def test_filter_consumer():
    """Test filtering rows from a data stream."""
    collector = Collector()
    pred = Col(columns='A', colidx=0) > 3
    consumer = Filter(predicate=pred, consumer=collector)
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    rows = consumer.close()
    assert len(rows) == 2
    assert rows[0] == (2, [4, 5, 6])
    assert rows[1] == (1, [7, 8, 9])
