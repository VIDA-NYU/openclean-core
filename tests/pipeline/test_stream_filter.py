# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the filter operator in data processing pipeline."""

import pytest

from openclean.function.eval.aggregate import Max
from openclean.function.eval.base import Col


def test_filter_rows_from_stream(ds):
    """Test filtering rows where from a stream."""
    # Filter rows where the value for 'B' is lower than the value for 'C'.
    df = ds.filter(Col('B') < Col('C')).to_df()
    assert df.shape == (5, 3)
    assert list(df['B']) == [0, 1, 2, 3, 4]
    assert list(df['C']) == [9, 8, 7, 6, 5]


def test_filter_from_stream_with_prepare(ds):
    """Test filtering rows with a evaluation function that needs to be
    prepared. A value function that needs to be prepared will raise a
    runtime error.
    """
    with pytest.raises(RuntimeError):
        ds.filter(Col('A') == Max('B')).count()
