# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the filter operator in data processing pipeline."""

from openclean.function.eval.base import Col


def test_update_rows_in_stream(ds):
    """Test updating values in a column in a data stream."""
    col_a = list(ds.update('A', Col('B') + Col('C')).to_df()['A'])
    assert col_a == [9] * 10
