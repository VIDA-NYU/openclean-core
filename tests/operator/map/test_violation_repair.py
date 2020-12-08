# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the constraint violations repair operator."""

import pandas as pd

from openclean.operator.map.violations import fd_violations
from openclean.operator.map.repair import Min, Max, conflict_repair


"""Test data frame with two groups that violate the FD: A -> B, C."""
DATASET = pd.DataFrame(
    data=[
        [1, 2, 3, 1],
        [1, 2, 3, 2],
        [2, 1, 4, 3],
        [2, 1, 2, 4],
        [2, 3, 1, 5],
        [3, 7, 8, 6],
        [3, 6, 8, 7],
        [3, 6, 9, 8],
    ],
    index=[8, 7, 6, 5, 4, 3, 2, 1],
    columns=['A', 'B', 'C', 'D']
)


def test_fd_violation_repair():
    """Test repair for FD A -> BC. Use Min to resolve conflicts in attribute B
    and Max to resolve conflicts in attribute C.
    """
    conflicts = fd_violations(DATASET, lhs='A', rhs=['B', 'C'])
    assert conflicts.keys() == set({2, 3})
    df = conflict_repair(conflicts=conflicts, strategy={'B': Min(), 2: Max()})
    assert list(df.index) == [8, 7, 6, 5, 4, 3, 2, 1]
    assert list(df['A']) == [1, 1, 2, 2, 2, 3, 3, 3]
    assert list(df['B']) == [2, 2, 1, 1, 1, 6, 6, 6]
    assert list(df['C']) == [3, 3, 4, 4, 4, 9, 9, 9]
    assert list(df['D']) == list(range(1, 9))
