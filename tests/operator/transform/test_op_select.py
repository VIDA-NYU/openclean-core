# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the column select operator."""

import pytest

from openclean.data.column import Column
from openclean.operator.transform.rename import rename
from openclean.operator.transform.select import select


def test_select_columns(employees):
    """Test selection a list of columns from a data frame."""
    # Select two columns
    d1 = select(employees, columns=['Name', 'Age'])
    assert 'Name' in d1.columns
    assert 'Age' in d1.columns
    assert 'Salary' not in d1.columns
    assert d1.shape == (7, 2)
    # Ensure that all columns are of type Columns and not strings
    for col in d1.columns:
        assert isinstance(col, Column)
    # Select columns using a mix of index and column name
    d1 = select(employees, columns=[0, 'Age'])
    assert 'Name' in d1.columns
    assert 'Age' in d1.columns
    assert 'Salary' not in d1.columns
    assert d1.shape == (7, 2)
    # Select single column (without renaming)
    d1 = select(employees, 'Name')
    assert 'Name' in d1.columns
    assert d1.shape == (7, 1)
    for col in d1.columns:
        assert isinstance(col, Column)
    # Select single column (with renaming)
    d1 = select(employees, columns='Name', names='Person')
    assert 'Person' in d1.columns
    assert d1.shape == (7, 1)
    for col in d1.columns:
        assert isinstance(col, Column)


def test_select_error_cases(employees):
    """Test error cases for invalid arguments."""
    # Error if a column index is specified that is out of range for the data
    # frame schema
    with pytest.raises(ValueError):
        select(employees, columns=[10, 'Salary'])
    # Error if an unknown column name is specified
    with pytest.raises(ValueError):
        select(employees, columns=['Name', 'Income'])
    # Error when list of selected columns names and result column names do not
    # match in length.
    with pytest.raises(ValueError):
        select(employees, columns=['Name', 'Salary'], names=['Person'])


def test_select_with_duplicates(employees):
    """Test selecting columns from data frames with duplicate column names."""
    # Rename first column 'Name' to 'Salary'
    d1 = rename(employees, 'Name', 'Salary')
    assert d1.columns[0] == 'Salary'
    assert d1.columns[1] == 'Age'
    assert d1.columns[2] == 'Salary'
    # Select the two 'Salary columns'
    d2 = select(d1, [0, 2])
    assert d2.columns[0] == 'Salary'
    assert d2.columns[1] == 'Salary'
    assert d2.iloc[0, 0] == 'Alice'
    assert d2.iloc[0, 1] == 60000


def test_select_with_rename(employees):
    """Test selecting columns and renaming columns."""
    d1 = select(
        employees,
        columns=['Name', 'Salary'],
        names=['Person', 'Income']
    )
    assert 'Person' in d1.columns
    assert 'Income' in d1.columns
    assert 'Name' not in d1.columns
    assert 'Age' not in d1.columns
    assert 'Salary' not in d1.columns
    assert d1.shape == (7, 2)
    # Ensure that all columns are of type Columns and not strings
    for col in d1.columns:
        assert isinstance(col, Column)
