# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""unit tests for the insert column operator."""

import math
import pytest

from openclean.data.types import Column
from openclean.function.eval.base import Col, Cols
from openclean.function.eval.string import Upper

from openclean.operator.transform.insert import inscol, insrow


def test_insert_multiple_columns(employees):
    """Test inserting multiple columns."""
    # Enter None as the default column value.
    df = inscol(employees, ['Height', 'Weight'])
    assert df.shape == (7, 5)
    for _, values in df.iterrows():
        assert values[1] is not None
        assert values[3] is None
        assert values[4] is None
    # The first three columns i the resulting data frame are instances of
    # Column and the new columns are strings
    for col in df.columns[0:3]:
        assert isinstance(col, Column)
    for col in df.columns[3:]:
        assert not isinstance(col, Column)
    # Insert columns with values from existing columns.
    df = inscol(
        employees,
        names=['Height', 'Weight'],
        pos=1,
        values=Cols('Name', 'Age')
    )
    assert df.shape == (7, 5)
    for _, values in df.iterrows():
        assert values[0] == values[1]
        assert values[2] == values[3] or math.isnan(values[2])
    # Error if number of returned values by the value function does not match
    # the number of inserted columns.
    with pytest.raises(ValueError):
        inscol(employees, ['Height', 'Weight'], pos=1, values=Col('Name'))


def test_insert_single_column(employees):
    """Test inserting a single column into a data frame."""
    df = inscol(employees, 'Name_Upper', pos=0, values=Upper('Name'))
    assert df.shape == (7, 4)
    for _, values in df.iterrows():
        assert values[0] == values[1].upper()
    # The first three columns i the resulting data frame are instances of
    # Column and the new columns are strings
    for col in df.columns[1:4]:
        assert isinstance(col, Column)


def test_insert_rows(employees):
    """Test insert rows operator."""
    df = insrow(employees, pos=1, values=['Paula', 23, 35])
    assert list(df.index) == [0, -1, 1, 2, 3, 4, 5, 6]
    df = insrow(employees, values=[['Paula', 23, 35], ['James', '34', 45.5]])
    assert list(df.index) == [0, 1, 2, 3, 4, 5, 6, -1, -1]
    # Error case
    with pytest.raises(ValueError):
        insrow(employees, pos=1, values=['Paula', 35])
    with pytest.raises(ValueError):
        insrow(employees, pos=1, values=[['Paula', 32, 35], ['James', '34']])
