# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for editing a dataset in the spreadsheet view."""

import pandas as pd
import pytest

from openclean.data.archive.base import VolatileArchiveManager
from openclean.engine.base import OpencleanEngine
from openclean.engine.library import ObjectLibrary
from openclean.engine.operator import StreamOp
from openclean.operator.stream.consumer import StreamFunctionHandler
from openclean.operator.stream.processor import StreamProcessor


# -- Fixtures -----------------------------------------------------------------

@pytest.fixture
def engine():
    """Get a openclean engine with two registered commands and one dataset 'DS'
    with person names.
    """
    df = pd.DataFrame(
        data=[[1, 'alice', 'JONES'], [2, 'bob', 'PETERS']],
        columns=['ID', 'FNAME', 'LNAME']
    )
    engine = OpencleanEngine(
        identifier='0000',
        manager=VolatileArchiveManager(),
        library=ObjectLibrary(),
        basedir=None,
        cached=True
    )
    engine.create(source=df, name='DS', primary_key='ID')
    engine.register.eval()(str.lower)
    engine.register.eval()(str.upper)
    return engine


# -- Stream operators ---------------------------------------------------------

class FuncProcessor(StreamProcessor):
    def __init__(self, func):
        self.func = func

    def open(self, schema):
        return StreamFunctionHandler(columns=schema, func=self.func)


def fname_to_upper(row):
    row[1] = row[1].upper()
    return row


def id_to_string(row):
    row[0] = 'ONE' if row[0] == 1 else 'TWO'
    return row


def lname_to_lower(row):
    row[2] = row[2].lower()
    return row


# -- Unit tests ---------------------------------------------------------------

def test_apply_pipeline(engine):
    """Test applying dataset operations directly on an archive snapshot."""
    # -- Empty pipeline -------------------------------------------------------
    operations = []
    snapshots = engine.apply('DS', operations=operations, validate=True)
    assert snapshots == []
    # -- Single operation -----------------------------------------------------
    operations = StreamOp(FuncProcessor(id_to_string), description='to_string')
    snapshots = engine.apply('DS', operations=operations, validate=True)
    assert len(snapshots) == 1
    assert snapshots[0].description == 'to_string'
    # -- List of operators ----------------------------------------------------
    operations = [StreamOp(FuncProcessor(fname_to_upper)), FuncProcessor(lname_to_lower)]
    snapshots = engine.apply('DS', operations=operations, validate=True)
    assert len(snapshots) == 2
    df = engine.checkout('DS')
    expected_result = pd.DataFrame(
        data=[['ONE', 'ALICE', 'jones'], ['TWO', 'BOB', 'peters']],
        columns=['ID', 'FNAME', 'LNAME']
    )
    pd.testing.assert_frame_equal(df, expected_result)


def test_full_dataset_operations(engine):
    """Test operations of a full dataset."""
    ds = engine.dataset('DS')
    ds.update(columns='FNAME', func=engine.library.functions().get_object('upper'))
    df = ds.update(columns='LNAME', func=engine.library.functions().get_object('lower'))
    log = ds.log()
    assert len(log) == 3
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['jones', 'peters']


def test_dataset_sample_operations(engine):
    """Test operations on a sample of the dataset."""
    original = engine.dataset('DS')
    engine.sample('DS', n=2)
    ds = engine.dataset('DS')
    ds.update(columns='FNAME', func=engine.library.functions().get_object('upper'))
    ds.insert(names='C', values=1)
    df = ds.update(columns='LNAME', func=engine.library.functions().get_object('lower'))
    # The samples dataset has changed.
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['jones', 'peters']
    assert list(df['C']) == [1, 1]
    # The current snapshot of the original archive has not changed.
    df = original.store.checkout()
    assert len(df.columns) == 3
    assert list(df['FNAME']) == ['alice', 'bob']
    assert list(df['LNAME']) == ['JONES', 'PETERS']
    # Checkout the latest version with commit.
    df = engine.checkout('DS', commit=True)
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['jones', 'peters']
    assert list(df['C']) == [1, 1]
    df = original.store.checkout()
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['jones', 'peters']
    assert list(df['C']) == [1, 1]


def test_dataset_sample_rollback(engine):
    """Test rollback operations on a sample of the dataset."""
    engine.sample('DS', n=2)
    ds = engine.dataset('DS')
    ds.update(columns='FNAME', func=engine.library.functions().get_object('upper'))
    ds.insert(names='C', values=1)
    ds.update(columns='LNAME', func=engine.library.functions().get_object('lower'))
    log = list(ds.log())
    assert len(log) == 4
    # Test error cases for rollback with invalid versions.
    with pytest.raises(KeyError):
        ds.rollback('undefined')
    # Rollback to the second operation.
    ds.rollback(log[2].version)
    assert len(ds.log()) == 3
    df = engine.checkout('DS', commit=True)
    assert len(engine.dataset('DS').log()) == 3
    assert len(df.columns) == 4
    assert list(df['FNAME']) == ['ALICE', 'BOB']
    assert list(df['LNAME']) == ['JONES', 'PETERS']
