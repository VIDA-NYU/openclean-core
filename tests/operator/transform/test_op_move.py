# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for column and row move operators."""

from openclean.operator.transform.move import movecols


def test_move_columns_without_rename(schools):
    """Test move column operator in data frame without duplicates."""
    # borough,school_code,grade,avg_class_size,min_class_size,max_class_size
    df = movecols(schools, columns=['avg_class_size', 'borough'], pos=1)
    assert df.columns[0] == 'school_code'
    assert df.columns[1] == 'avg_class_size'
    assert df.columns[2] == 'borough'
    assert set(df['borough']) == set({'K', 'M', 'Q', 'R', 'X'})


def test_move_columns_with_rename(dupcols):
    """Test move column operator in data frame with duplicates."""
    # borough,school_code,grade,avg_class_size,min_class_size,max_class_size
    df = movecols(dupcols, columns=[1], pos=0)
    assert list(df.columns) == ['A', 'Name', 'A']
