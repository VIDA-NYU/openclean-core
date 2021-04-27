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

from openclean.data.groupby import ConflictSummary, DataFrameViolation


@pytest.fixture
def dataset():
    return pd.DataFrame(
        data=[
            ['Alice', 23, 40000],
            ['Alice', 23, 45000],
            ['Bob', 32, 42000],
            ['Bob', 23, 42000]
        ],
        columns=['Name', 'Age', 'Salary']
    )


def test_conflict_summary_object(dataset):
    """Test conflict summary object."""
    conflicts = ConflictSummary()
    conflicts.add([])
    conflicts.add(['Alice'])
    conflicts.add(['Alice', 'Bob'])
    conflicts.add(['Alice', 'Bob', 'Claire'])
    conflicts.add(['Claire', 'Dave'])
    assert set(conflicts.keys()) == {'Alice', 'Bob', 'Claire', 'Dave'}
    alice = conflicts['Alice']
    assert alice.count == 3
    assert alice.values == Counter(['Bob', 'Bob', 'Claire'])


def test_data_frame_conflicts(dataset):
    """Test getting conflicting values for groups in a grouping."""
    violations = DataFrameViolation(df=dataset)
    violations.add('Alice', [0, 1]).add('Bob', [2, 3])
    assert violations.conflicts(key='Alice', columns='Age') == Counter({23: 2})
    assert violations.conflicts(key='Bob', columns='Age') == Counter({23: 1, 32: 1})
    conflicts = violations.conflicts(key='Alice', columns=['Age', 'Salary'])
    assert conflicts == Counter({(23, 40000): 1, (23, 45000): 1})


def test_data_frame_conflict_summary(dataset):
    """Test getting conflicting values for groups in a grouping."""
    violations = DataFrameViolation(df=dataset)
    violations.add('Alice', [0, 1]).add('Bob', [2, 3])
    summary = violations.summarize_conflicts(columns='Age')
    assert set(summary.keys()) == {23, 32}
    assert summary[23].count == 1
    assert summary[23].values == Counter([32])
    assert summary[32].count == 1
    assert summary[32].values == Counter([23])
    summary = violations.summarize_conflicts(columns=['Age', 'Salary'])
    assert set(summary.keys()) == {(23, 40000), (23, 45000), (23, 42000), (32, 42000)}
    for key in summary:
        assert len(summary[key].values) == 1
