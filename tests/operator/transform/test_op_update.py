# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import math
import pytest

from openclean.data.column import Column
from openclean.function.column import Col
from openclean.function.predicate.comp import Eq
from openclean.function.predicate.datatype import IsNaN
from openclean.function.predicate.logic import And
from openclean.function.replace import IfThenReplace, Lookup
from openclean.function.value.replace import lookup

from openclean.operator.transform.filter import filter
from openclean.operator.transform.update import swap, update


BOROUGHS = ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']


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
    df = update(nyc311, 'borough', Lookup('borough', mapping))
    # Ensure that the columns are instances of the Column class
    for col in df.columns:
        assert isinstance(col, Column)
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(mapping.values())
    df = update(nyc311, 'borough', lookup(mapping))
    # Ensure that the columns are instances of the Column class
    for col in df.columns:
        assert isinstance(col, Column)
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(mapping.values())
    # Mapping with missing values
    mapping = {
        'BROOKLYN': 'BK',
        'MANHATTAN': 'MN',
        'QUEENS': 'QN',
        'STATEN ISLAND': 'SI'
    }
    df = update(nyc311, 'borough', Lookup('borough', mapping))
    values = df['borough'].unique()
    assert len(values) == 5
    assert sorted(values) == sorted(list(mapping.values()) + ['BRONX'])
    # Raise error for missing value
    with pytest.raises(KeyError):
        update(nyc311, 'borough', Lookup('borough', mapping, raise_error=True))
    # Multi column lookup
    cond = And(Eq('descriptor', 'Stop'), Eq('borough', 'BROOKLYN'))
    assert filter(nyc311, cond).shape == (19, 4)
    mapping = {('Stop', 'BROOKLYN'): 'Stopping in Brooklyn'}
    boroughs = lookup(mapping, for_missing=0)
    df = update(
        nyc311,
        'descriptor',
        Lookup(['descriptor', 'borough'], boroughs)
    )
    # Ensure that the columns are instances of the Column class
    for col in df.columns:
        assert isinstance(col, Column)
    assert filter(df, cond).shape == (0, 4)
    cond = And(
        Eq('descriptor', 'Stopping in Brooklyn'),
        Eq('borough', 'BROOKLYN')
    )
    assert filter(df, cond).shape == (19, 4)


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


def test_update_with_conditional_replace(employees):
    """Simple test of a conditional replacement accross columns in a data frame
    row.
    """
    df = update(employees, 'Salary', IfThenReplace(IsNaN('Age'), 0))
    assert math.isnan(df.iloc[3][1])
    assert df.iloc[3][2] == 0
