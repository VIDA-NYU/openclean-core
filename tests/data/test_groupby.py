# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for the data frame grouping class."""

from collections import Counter

import pandas as pd
import pytest

from openclean.data.groupby import DataFrameGrouping


COLS = ['A', 'B']
R1 = [1, 2]
R2 = [3, 4]
R3 = [5, 6]
R4 = [7, 8]
R5 = [9, 10]


@pytest.fixture
def grouping():
    df = pd.DataFrame(
        data=[R1, R2, R3, R4, R5],
        index=[3, 2, 1, -1, 0],
        columns=COLS
    )
    return DataFrameGrouping(df=df)


def test_add_and_get_groups(grouping):
    """Test adding and accessing data frame groups."""
    grouping.add('A', [2, 1]).add('B', [3, 0]).add('C', [2, 1, 3])
    assert len(grouping) == 3
    # Error when adding group for existing key.
    with pytest.raises(ValueError):
        grouping.add('B', [1])
    # Group: A
    df = pd.DataFrame(data=[R2, R3], index=[2, 1], columns=COLS)
    assert grouping.rows('A') == [2, 1]
    assert grouping.get('A').equals(df)
    # Group: B
    df = pd.DataFrame(data=[R1, R4], index=[3, -1], columns=COLS)
    assert grouping.rows('B') == [3, 0]
    assert grouping.get('B').equals(df)
    # Group: C
    df = pd.DataFrame(data=[R2, R3, R4], index=[2, 1, -1], columns=COLS)
    assert grouping.rows('C') == [2, 1, 3]
    assert grouping.get('C').equals(df)
    # Non-existing key
    assert grouping.get('D') is None
    assert grouping.rows('D') is None


def test_group_keys(grouping):
    """Test keys() method of the data frame grouping."""
    grouping.add('A', [1, 2]).add(('B', 'C'), [0, 3]).add('C', [1, 2, 3])
    assert len(grouping) == 3
    # Error when adding group for existing key.
    with pytest.raises(ValueError):
        grouping.add(('B', 'C'), [1])
    assert grouping.keys() == set({'A', ('B', 'C'), 'C'})
    assert list(grouping) == ['A', ('B', 'C'), 'C']


def test_group_values(grouping):
    """Test values() method of the data frame grouping."""
    grouping.add('A', [1, 2]).add(('B', 'C'), [0, 3]).add('C', [1, 2, 3])
    assert grouping.values('A', columns='B') == Counter({4: 1, 6: 1})
    grouping.values(('B', 'C'), columns=['A', 'B']) == Counter({(1, 2): 1, (9, 10): 1})
    assert grouping.values('D', columns=['A']) is None


def test_iterate_over_groups(grouping):
    """Test adding and accessing data frame groups."""
    grouping.add('A', [2, 1]).add('B', [4, 0]).add('C', [2, 1, 0])
    col_a = dict()
    for key, df in grouping.items():
        col_a[key] = list(df['A'])
    assert col_a == {'A': [3, 5], 'B': [1, 9], 'C': [1, 3, 5]}
    for key, df in grouping.groups():
        col_a[key] = list(df['A'])
    assert col_a == {'A': [3, 5], 'B': [1, 9], 'C': [1, 3, 5]}
