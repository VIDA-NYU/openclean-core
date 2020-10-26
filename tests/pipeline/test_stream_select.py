# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the select operator in data processing pipelines."""

import pytest


def test_select_columns_from_stream(ds):
    """Test selecting columns from a data stream."""
    df = ds.select(['A', 'C']).to_df()
    assert df.shape == (10, 2)
    assert list(df.columns) == ['A', 'C']
    assert list(df.iloc[0]) == ['A', 9]


def test_rename_columns_from_stream(ds):
    """Test selecting and renaming columns from a data stream."""
    df = ds.select(['A', 'C'], names=['C1', 'C2']).to_df()
    assert df.shape == (10, 2)
    assert list(df.columns) == ['C1', 'C2']
    assert list(df.iloc[0]) == ['A', 9]


def test_rename_columns_error(ds):
    """Test error when providing incompatible lists for selected and renamed
    columns.
    """
    with pytest.raises(ValueError):
        ds.select(['A', 'C'], names=['C1']).to_df()
