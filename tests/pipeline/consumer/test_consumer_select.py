# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Select consumer for data streams."""

from openclean.pipeline.consumer.collector import Collector
from openclean.pipeline.consumer.producer import Select


def test_column_select():
    """Test filtering columns in data stream rows."""
    collector = Collector()
    consumer = Select(columns=[2, 1], consumer=collector)
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    rows = consumer.close()
    assert len(rows) == 3
    assert rows[0] == (3, [3, 2])
    assert rows[1] == (2, [6, 5])
    assert rows[2] == (1, [9, 8])
