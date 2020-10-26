# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the insert column operator in data processing pipelines."""

from openclean.function.eval.base import Col


def test_insert_multiple_columns_into_stream(ds):
    """Test inserting two new columns into a data stream."""
    df = ds.insert(['D', 'E'], pos=1, values=[1, Col('B') + Col('C')]).to_df()
    assert list(df.columns) == ['A', 'D', 'E', 'B', 'C']
    assert list(df['D']) == [1] * 10
    assert list(df['E']) == [9] * 10


def test_insert_single_column_into_stream(ds):
    """Test inserting a new column into a data stream."""
    df = ds.insert('D', pos=1, values=1).to_df()
    assert list(df.columns) == ['A', 'D', 'B', 'C']
    assert list(df['D']) == [1] * 10
