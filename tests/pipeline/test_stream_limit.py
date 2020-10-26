# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the limit operator in data processing pipeline."""

import pytest


@pytest.mark.parametrize('limit,count', [(0, 0), (5, 5), (10, 10), (15, 10)])
def test_stream_head(limit, count, ds):
    """Test limiting the number of rows that are passed on to a downstream
    consumer.
    """
    df = ds.head(limit)
    assert list(df.columns) == ['A', 'B', 'C']
    assert df.shape == (count, 3)


@pytest.mark.parametrize('limit,count', [(0, 0), (5, 5), (10, 10), (15, 10)])
def test_stream_limit(limit, count, ds):
    """Test limiting the number of rows that are passed on to a downstream
    consumer.
    """
    assert ds.limit(limit).count() == count
