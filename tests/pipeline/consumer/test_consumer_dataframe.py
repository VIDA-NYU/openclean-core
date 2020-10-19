# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the DataFrame consumer for data streams."""

from openclean.pipeline.consumer.collector import DataFrame


def test_data_frame_consumer():
    """Test creating a data frame from a sequence of rows."""
    columns = ['A', 'B', 'C']
    consumer = DataFrame(columns=columns)
    consumer.consume(3, [1, 2, 3])
    consumer.consume(2, [4, 5, 6])
    consumer.consume(1, [7, 8, 9])
    df = consumer.close()
    assert list(df.columns) == columns
    assert list(df.index) == [3, 2, 1]
    assert list(df['A']) == [1, 4, 7]
