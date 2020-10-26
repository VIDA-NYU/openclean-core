# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Count consumer for data streams."""

from openclean.operator.stream.collector import RowCount


def test_row_counts():
    """Test the stream consumer that counts the number of rows in a stream."""
    consumer = RowCount().open([])
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    rows = consumer.close()
    assert rows == 3
