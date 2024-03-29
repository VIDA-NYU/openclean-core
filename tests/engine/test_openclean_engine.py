# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the basic functionality of the openclean engine."""

import pytest

from openclean.engine.base import DB


@pytest.mark.parametrize('cached', [True, False])
def test_recreate_engine(cached, dataset, tmpdir):
    """Test creating and deleting datasets via the engine and accessing them
    after re-creating the engine.
    """
    engine = DB(basedir=str(tmpdir), create=True)
    # Create two datasets with a single snapshot.
    df = engine.create(source=dataset, name='My Data').checkout()
    assert df.shape == (2, 3)
    df = engine.create(source=df[df['A'] == 1], name='More Data').checkout()
    assert df.shape == (1, 3)
    # Re-create the engine and access the snapshots.
    engine = DB(basedir=str(tmpdir), create=False, cached=cached)
    df = engine.checkout(name='My Data')
    assert df.shape == (2, 3)
    df = engine.checkout(name='More Data')
    assert df.shape == (1, 3)


def test_dataset_stream(dataset, tmpdir):
    """Test stream operations on a dataset snapshot."""
    engine = DB(basedir=str(tmpdir), create=True)
    # Create two datasets with a single snapshot.
    engine.create(source=dataset, name='My Data').checkout()
    df = engine.stream('My Data').to_df()
    assert df.shape == (2, 3)


def test_full_df_checkout(dataset, tmpdir):
    """Test if a full dataset checkout is possible"""
    db = DB(basedir=str(tmpdir), create=True)
    # Create two datasets with a single snapshot.
    df = db.create(source=dataset, name='test').checkout()
    assert df.shape == (2, 3)

    df1 = df.copy(deep=True)
    df1['A'] = df1['A'] + 1
    db.commit(name='test', source=df1)
    snapshots = db.dataset('test').log()

    df = db.dataset('test').checkout(version=snapshots[1].version)
    assert list(df['A']) == list(df1['A'])

    df = db.dataset('test').checkout(version=snapshots[0].version)
    assert list(df['A']) == list(dataset['A'])


def test_full_df_rollback(dataset, tmpdir):
    """Test rollback of a full dataset."""
    db = DB(basedir=str(tmpdir), create=True)
    # Create dataset with two snapshot.
    df = db.create(source=dataset, name='test').checkout()
    df1 = df.copy(deep=True)
    df1['A'] = df1['A'] + 1
    db.commit(name='test', source=df1)
    snapshots = db.dataset('test').log()
    df = db.dataset('test').checkout(version=snapshots[1].version)
    assert list(df['A']) == list(df1['A'])
    # Remove last snapshot by rollback to version 0.
    df = db.rollback('test', version=snapshots[0].version)
    assert list(df['A']) == list(dataset['A'])
