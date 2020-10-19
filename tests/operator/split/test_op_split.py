# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the split operator."""

from openclean.data.types import Column
from openclean.function.eval.base import Col, Eq
from openclean.operator.split.split import split


def test_split_operator(nyc311):
    """Simple test for split operator."""
    # Divide the data frame into one set with 20 rows (matching borough STATEN
    # ISLAND) and one data frame with the remaining rows.
    df_true, df_false = split(nyc311, Eq(Col('borough'), 'STATEN ISLAND'))
    assert df_true.shape == (20, len(nyc311.columns))
    assert df_false.shape == (nyc311.shape[0] - 20, len(nyc311.columns))
    # Ensure that the columns are instances of the Column class
    for col in df_true.columns:
        assert isinstance(col, Column)
    for col in df_false.columns:
        assert isinstance(col, Column)
