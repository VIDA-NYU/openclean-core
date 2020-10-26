# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the delete operator in data processing pipelines."""

from openclean.function.eval.base import Col


def test_delete_rows_from_stream(ds):
    """Test deleting rows where from a stream."""
    # Delete rows where the value for 'B' is lower than the value for 'C'.
    df = ds.delete(Col('B') < Col('C')).to_df()
    assert df.shape == (5, 3)
    assert list(df['B']) == [5, 6, 7, 8, 9]
    assert list(df['C']) == [4, 3, 2, 1, 0]
