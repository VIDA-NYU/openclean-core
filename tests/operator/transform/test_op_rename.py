# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the column rename operators."""

import pytest

from openclean.data.types import Column
from openclean.operator.transform.rename import rename


def test_rename_columns(dupcols):
    """Test renaming columns in a data frame with duplicate column names."""
    # Rename the first column
    d1 = rename(dupcols, columns='Name', names='Person')
    assert d1.columns[0] == 'Person'
    assert dupcols.columns[0] == 'Name'
    assert d1.columns[1] == 'A'
    assert d1.columns[2] == 'A'
    for col in d1.columns:
        assert isinstance(col, Column)
    assert d1.shape == (7, 3)
    # Rename the first column and the second column
    d1 = rename(dupcols, columns=['Name', 'A'], names=['Person', 'Col2'])
    assert d1.columns[0] == 'Person'
    assert d1.columns[1] == 'Col2'
    assert d1.columns[2] == 'A'
    for col in d1.columns:
        assert isinstance(col, Column)
    assert d1.shape == (7, 3)
    # Rename the first column and the last column
    d1 = rename(dupcols, columns=['Name', 2], names=['Person', 'Col2'])
    assert d1.columns[0] == 'Person'
    assert d1.columns[1] == 'A'
    assert d1.columns[2] == 'Col2'
    for col in d1.columns:
        assert isinstance(col, Column)
    assert d1.shape == (7, 3)


def text_rename_column_errors(dupcols):
    """Test error cases for invalid arguments."""
    # Incompatible column and name lists
    with pytest.raises(ValueError):
        rename(dupcols, columns='Name', names=['Person', 'Age'])
    # Unknown column name
    with pytest.raises(ValueError):
        rename(dupcols, columns='Names', names='Persons')
    # Column index out of range
    with pytest.raises(ValueError):
        rename(dupcols, columns=100, names='Persons')
