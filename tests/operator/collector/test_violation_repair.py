# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the constraint violations repair operator."""

from collections import Counter

import pandas as pd
import pytest

from openclean.operator.map.violations import fd_violations
from openclean.operator.collector.repair import Min, Max, conflict_repair


"""Test data frame with two groups that violate the FD: A -> B, C."""
DATASET = pd.DataFrame(
    data=[
        [2, 1, 4, 1],
        [2, 1, 2, 2],
        [2, 3, 1, 3],
        [1, 2, 3, 4],
        [1, 2, 3, 5],
        [3, 7, 8, 6],
        [3, 6, 8, 7],
        [3, 6, 9, 8]
    ],
    index=[8, 7, 6, 5, 4, 3, 2, 1],
    columns=['A', 'B', 'C', 'D']
)

COL_A = [2, 2, 2, 1, 1, 3, 3, 3]
COL_C = [4, 2, 1, 3, 3, 8, 8, 9]
UPD_COL_B = [1, 1, 1, 2, 2, 6, 6, 6]
UPD_COL_C = [4, 4, 4, 3, 3, 9, 9, 9]

CONFIG = [({'B': Min()}, COL_C), ({'B': Min(), 2: Max()}, UPD_COL_C)]


@pytest.mark.parametrize('strategy,col_c', CONFIG)
def test_fd_violation_repair_in_order(strategy, col_c):
    """Test repair for FD A -> BC. Use Min to resolve conflicts in attribute B
    and Max to resolve conflicts in attribute C. Violations are repaired in
    order so that the order of rows in the input data frame is the same as in
    the resulting data frame.
    """
    conflicts = fd_violations(DATASET, lhs='A', rhs=['B', 'C'])
    assert conflicts.keys() == set({2, 3})
    df = conflict_repair(conflicts=conflicts, strategy=strategy, in_order=True)
    assert list(df.index) == [8, 7, 6, 5, 4, 3, 2, 1]
    assert list(df['A']) == COL_A
    assert list(df['B']) == UPD_COL_B
    assert list(df['C']) == col_c
    assert list(df['D']) == list(range(1, 9))
    # Assert that the original data frame is unchanged.
    assert list(DATASET['B']) == [1, 1, 3, 2, 2, 7, 6, 6]


@pytest.mark.parametrize('strategy,col_c', CONFIG)
def test_fd_violation_repair_with_append(strategy, col_c):
    """Test repair for FD A -> BC. Use Min to resolve conflicts in attribute B
    and Max to resolve conflicts in attribute C. Violations are repaired using
    the startegy that does not guarantee that the rows in the resulting data
    frame are in the same order as the rows in the input data frame.
    """
    conflicts = fd_violations(DATASET, lhs='A', rhs=['B', 'C'])
    assert conflicts.keys() == set({2, 3})
    df = conflict_repair(conflicts=conflicts, strategy=strategy, in_order=False)
    assert sorted(list(df.index)) == list(range(1, 9))
    assert Counter(list(df['A'])) == Counter(COL_A)
    assert Counter(list(df['B'])) == Counter(UPD_COL_B)
    assert Counter(list(df['C'])) == Counter(col_c)
    assert sorted(list(df['D'])) == list(range(1, 9))
    # Assert that the original data frame is unchanged.
    assert list(DATASET['B']) == [1, 1, 3, 2, 2, 7, 6, 6]
