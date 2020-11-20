# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the basic functionality of the openclean engine."""

import pytest

from openclean.engine.base import DB


@pytest.mark.parametrize('cached', [True, False])
def test_create_and_delete_dataset(cached, persistent_engine, dataset):
    """Test creating and deleting datasets via the engine."""
    # Create and checkout a dataset snapshot.
    df = persistent_engine.create(source=dataset, name='My Data')
    assert df.shape == (2, 3)
    df = persistent_engine.checkout(name='My Data')
    assert df.shape == (2, 3)
    # Cannot create multiple datasets with the same name.
    with pytest.raises(ValueError):
        persistent_engine.create(source=dataset, name='My Data')
    # Create a second dataset.
    df = persistent_engine.create(source=df[df['A'] == 1], name='More Data')
    assert df.shape == (1, 3)
    df = persistent_engine.checkout(name='More Data')
    assert df.shape == (1, 3)
    df = persistent_engine.checkout(name='My Data')
    assert df.shape == (2, 3)
    # Drop the second dataset.
    persistent_engine.drop('More Data')
    df = persistent_engine.checkout(name='My Data')
    assert df.shape == (2, 3)
    with pytest.raises(ValueError):
        persistent_engine.drop('More Data')
    with pytest.raises(ValueError):
        persistent_engine.checkout(name='More Data')
    df = persistent_engine.checkout(name='My Data')
    assert df.shape == (2, 3)


@pytest.mark.parametrize('cached', [True, False])
def test_recreate_engine(cached, dataset, tmpdir):
    """Test creating and deleting datasets via the engine and accessing them
    after re-creating the engine.
    """
    engine = DB(basedir=str(tmpdir), create=True)
    # Create two datasets with a single snapshot.
    df = engine.create(source=dataset, name='My Data')
    assert df.shape == (2, 3)
    df = engine.create(source=df[df['A'] == 1], name='More Data')
    assert df.shape == (1, 3)
    # Re-create the engine and access the snapshots.
    engine = DB(basedir=str(tmpdir), create=False, cached=cached)
    df = engine.checkout(name='My Data')
    assert df.shape == (2, 3)
    df = engine.checkout(name='More Data')
    assert df.shape == (1, 3)
