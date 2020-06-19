# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import pytest

from openclean import vizual
from openclean.data.column import Column


def test_delete_columns(employees):
    """Test removing columns by their identifier."""
    # Delete a single column.
    colname = employees.columns[0]
    df = vizual.delete_columns(employees, colids=colname.colid)
    assert len(df.columns) == 2
    assert colname not in df.columns
    with pytest.raises(ValueError):
        vizual.delete_columns(df, colids=colname.colid)
    # Delete multiple columns.
    colage = employees.columns[1]
    df = vizual.delete_columns(employees, colids=[colname.colid, colage.colid])
    assert len(df.columns) == 1
    assert colname not in df.columns
    assert colage not in df.columns
    with pytest.raises(ValueError):
        vizual.delete_columns(df, colids=[df.columns[0].colid, -1])


def test_delete_rows(employees):
    """Delete rows by their identifier."""
    # Remove a single row.
    df = vizual.delete_rows(employees, rowids=0)
    assert len(df.index) == len(employees.index) - 1
    assert len(df.columns) == len(employees.columns)
    for col in df.columns:
        assert isinstance(col, Column)
    # Remove all remaining rows
    df1 = vizual.delete_rows(df, rowids=list(df.index))
    assert len(df1.index) == 0
    assert len(df.columns) == len(employees.columns)
    # Error case.
    with pytest.raises(ValueError):
        vizual.delete_rows(employees, rowids=[0, -1])


def test_filter_columns(employees):
    """Test filter columns by their identifier."""
    df = vizual.filter_columns(employees, colids=[0, 2])
    assert len(df.columns) == 2
    assert len(df.index) == len(employees.index)
    df = vizual.filter_columns(employees, colids=[1, 0], names=['A', 'B'])
    assert len(df.columns) == 2
    assert len(df.index) == len(employees.index)
    assert 'A' in df.columns
    assert 'B' in df.columns
    with pytest.raises(ValueError):
        vizual.filter_columns(employees, colids=[0, -2])


def test_insert_column(employees):
    """Test inserting a new column into a data frame."""
    df = vizual.insert_column(employees, names=['BDate'], pos=1)
    assert len(df.columns) == len(employees.columns) + 1
    assert len(df.index) == len(employees.index)
    with pytest.raises(ValueError):
        vizual.insert_column(employees, names=['BDate'], pos=10)


def test_insert_row(employees):
    """Test inserting a new row into a data frame."""
    df = vizual.insert_row(employees, pos=1)
    assert len(df.columns) == len(employees.columns)
    assert len(df.index) == len(employees.index) + 1
    with pytest.raises(ValueError):
        vizual.insert_row(employees, pos=100)


def test_rename_columns(employees):
    """Test renaming a column in a data frame."""
    df = vizual.rename_columns(employees, colids=1, names=['CurrAge'])
    assert len(df.columns) == len(employees.columns)
    assert len(df.index) == len(employees.index)
    assert 'Age' not in df.columns
    assert 'CurrAge' in df.columns
    with pytest.raises(ValueError):
        vizual.rename_columns(employees, colids=100, names=['CurrAge'])


def test_move_columns(employees):
    """Test moving columns."""
    df = vizual.move_columns(employees, colids=[0, 1], pos=2)
    assert list(df.columns) == ['Salary', 'Name', 'Age']


def test_move_rows(employees):
    """Test moving rows."""
    df = vizual.move_rows(employees, rowids=[6, 0], pos=2)
    assert list(df.index) == [1, 2, 6, 0, 3, 4, 5]


def test_sort_dataset(employees):
    """Test renaming a column in a data frame."""
    df = vizual.sort_dataset(employees, colids=0, reversed=True)
    assert list(df.index) == [6, 5, 4, 3, 2, 1, 0]


def test_update_cell(employees):
    """Test the VizUAL single cell update operator."""
    import numpy as np
    assert np.sum(employees['Age']) == 199.0
    df = vizual.update_cell(employees, colid=1, rowid=3, value=21)
    assert np.sum(df['Age']) == 220.0
    with pytest.raises(ValueError):
        vizual.update_cell(employees, colid=100, rowid=3, value=21)
    with pytest.raises(ValueError):
        vizual.update_cell(employees, colid=1, rowid=30, value=21)
