# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import pytest

from openclean.data.column import Column
from openclean.function.column import Col
from openclean.function.value.lookup import Lookup
from openclean.operator.transform.update import swap, update


BOROUGHS = [
    'BRONX',
    'BROOKLYN',
    'MANHATTAN',
    'QUEENS',
    'STATEN ISLAND'
]


def test_swap_columns(nyc311):
    """Test swapping values in two columns of a data frame."""
    df = swap(nyc311, 'borough', 'descriptor')
    # Ensure that the columns are instances of the Column class
    for col in df.columns:
        assert isinstance(col, Column)
    values = df['descriptor'].unique()
    assert len(values) == 5
    assert sorted(values) == BOROUGHS
    # Result is independent of the order of col1 and col2
    df = swap(nyc311, 'descriptor', 'borough')
    # Ensure that the columns are instances of the Column class
    for col in df.columns:
        assert isinstance(col, Column)
    values = df['descriptor'].unique()
    assert len(values) == 5
    assert sorted(values) == BOROUGHS
    # Error for invalid column list
    with pytest.raises(ValueError):
        swap(nyc311, ['descriptor', 'borough'], 'descriptor')


def test_update_single_column(nyc311):
    """Test updating values in a single column of a data frame."""
    mapping = {
        'BRONX': 'BX',
        'BROOKLYN': 'BK',
        'MANHATTAN': 'MN',
        'QUEENS': 'QN',
        'STATEN ISLAND': 'SI'
    }
    df = update(nyc311, 'borough', mapping)
    # Ensure that the columns are instances of the Column class
    for col in df.columns:
        assert isinstance(col, Column)
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(mapping.values())
    df = update(nyc311, 'borough', Lookup(mapping))
    # Ensure that the columns are instances of the Column class
    for col in df.columns:
        assert isinstance(col, Column)
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(mapping.values())


def test_update_multiple_columns(nyc311):
    """Test updating values in multiple columns of a data frame."""
    df = update(
        df=nyc311,
        columns=['borough', 'descriptor'],
        func=Col(['descriptor', 'borough'])
    )
    # Ensure that the columns are instances of the Column class
    for col in df.columns:
        assert isinstance(col, Column)
    values = df['descriptor'].unique()
    assert len(values) == 5
    assert sorted(values) == BOROUGHS
    # Error for non-matching (column, value) counts
    with pytest.raises(ValueError):
        update(nyc311, ['borough', 'descriptor'], Col('borough'))
