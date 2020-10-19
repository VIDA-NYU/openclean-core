# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the filter operator in data processing pipeline."""

import pandas as pd
import pytest

from openclean.data.stream.df import DataFrameStream
from openclean.function.eval.aggregate import Min, Max
from openclean.function.eval.base import Col
from openclean.pipeline.processor.collector import DataFrameOperator
from openclean.pipeline.processor.producer import FilterOperator


@pytest.fixture
def ds():
    """Get a data frame stream with two columns ('A', 'B') and ten rows such
    that values in 'A'range from 0 .. 9 and values in 'B' from 9 .. 0.
    """
    data = list()
    for i in range(10):
        data.append([i, 9 - i])
    df = pd.DataFrame(data=data, columns=['A', 'B'])
    return DataFrameStream(df)


def test_filter_rows_from_stream(ds):
    """Test filtering rows where from a stream."""
    # Filter rows where the value for 'A' is lower than the value for 'B'.
    df = FilterOperator(Col('A') < Col('B'))\
        .open(ds, ds.columns, downstream=[DataFrameOperator()])\
        .process(ds)
    assert df.shape == (5, 2)
    assert list(df['A']) == [0, 1, 2, 3, 4]
    assert list(df['B']) == [9, 8, 7, 6, 5]


def test_filter_from_stream_with_prepare(ds):
    """Test filtering rows with a evaluation function that needs to be
    prepared.
    """
    # Get the row where the value for 'A' equals the maximum for 'B'
    df = FilterOperator(Col('A') == Max('B'))\
        .open(ds, ds.columns, downstream=[DataFrameOperator()])\
        .process(ds)
    assert df.shape == (1, 2)
    assert list(df.iloc[0]) == [9, 0]
    # Use additional upstream filter to ensure that the function is
    # prepatred on the filtered data.
    op = FilterOperator(Col('A') <= 5)
    df = FilterOperator(Col('A') == Min('B'))\
        .open(ds, ds.columns, upstream=[op], downstream=[DataFrameOperator()])\
        .process(ds)
    assert df.shape == (1, 2)
    assert list(df.iloc[0]) == [4, 5]
