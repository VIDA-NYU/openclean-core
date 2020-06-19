# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the data frame sort operator."""

from openclean.operator.transform.sort import order_by


def test_orderby_without_rename(agencies):
    """Test sorting a data frame without duplicate column names."""
    df = order_by(agencies, columns=[0, 1])
    assert list(df.index) == [0, 4, 3, 8, 5, 2, 9, 7, 1, 6]
    df = order_by(agencies, columns=['agency', 1], reversed=[False, True])
    assert list(df.index) == [0, 4, 8, 3, 5, 7, 9, 2, 6, 1]


def test_orderby_with_rename(dupcols):
    """Test sorting a data frame with duplicate column names."""
    df = order_by(dupcols, columns=1, reversed=True)
    df.columns = ['A', 'B', 'C']
    assert list(df['B']) == [45, 44, 37, 34, 32, 29, 23]
