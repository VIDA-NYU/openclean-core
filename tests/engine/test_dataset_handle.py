# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for editing a dataset in the spreadsheet view."""

import pandas as pd
import pytest

from openclean.engine.base import DB


@pytest.fixture
def engine():
    """Get a openclean engine with two registered commands and one dataset 'DS'
    with person names.
    """
    df = pd.DataFrame(
        data=[[1, 'alice', 'JONES'], [2, 'bob', 'PETERS']],
        columns=['ID', 'FNAME', 'LNAME']
    )
    engine = DB()
    engine.create(source=df, name='DS', primary_key='ID')
    engine.register.eval()(str.lower)
    engine.register.eval()(str.upper)
    return engine


def test_full_dataset_operations(engine):
    """Test operations of a full dataset."""
    ds = engine.dataset('DS')
    ds.update(columns='FNAME', func=engine.library.get('upper'))
    df = ds.update(columns='LNAME', func=engine.library.get('lower'))
    log = ds.log()
    assert len(log) == 3
    for op in log:
        assert op.is_committed
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['jones', 'peters']


def test_dataset_sample_operations(engine):
    """Test operations on a sample of the dataset."""
    original = engine.dataset('DS')
    engine.sample('DS', n=2)
    ds = engine.dataset('DS')
    ds.update(columns='FNAME', func=engine.library.get('upper'))
    ds.insert(names='C', values=1)
    df = ds.update(columns='LNAME', func=engine.library.get('lower'))
    # The samples dataset has changed.
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['jones', 'peters']
    assert list(df['C']) == [1, 1]
    # The current snapshot of the original archive has not changed.
    df = original.datastore.checkout()
    assert len(df.columns) == 3
    assert list(df['FNAME']) == ['alice', 'bob']
    assert list(df['LNAME']) == ['JONES', 'PETERS']
    # Checkout the latest version with commit.
    df = engine.checkout('DS', commit=True)
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['jones', 'peters']
    assert list(df['C']) == [1, 1]
    df = original.datastore.checkout()
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['jones', 'peters']
    assert list(df['C']) == [1, 1]


def test_dataset_sample_rollback(engine):
    """Test rollback operations on a sample of the dataset."""
    engine.sample('DS', n=2)
    ds = engine.dataset('DS')
    ds.update(columns='FNAME', func=engine.library.get('upper'))
    ds.insert(names='C', values=1)
    ds.update(columns='LNAME', func=engine.library.get('lower'))
    log = list(ds.log())
    # Test error cases for rollback with invalid identifiers.
    with pytest.raises(ValueError):
        ds.rollback(log[0].identifier)
    with pytest.raises(KeyError):
        ds.rollback('undefined')
    # Rollback to the first operation.
    ds.rollback(log[2].identifier)
    ds.commit()
    df = engine.checkout('DS', commit=True)
    assert len(df.columns) == 3
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['JONES', 'PETERS']
