# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the cached data store."""

from openclean.data.archive.cache import CachedDatastore


def test_cache_metadata(dataset, store):
    """Test accessing metadata for a dataset in a cached datastore."""
    cached_store = CachedDatastore(datastore=store)
    cached_store.commit(df=dataset)
    cached_store.metadata()\
        .set_annotation(column_id=1, key='type', value='int')
    annos = cached_store.metadata(version=0)
    assert annos.get_annotation(column_id=1, key='type') == 'int'


def test_cache_dataframe(dataset, store):
    """Test maintaining the last dataset in the cache."""
    cached_store = CachedDatastore(datastore=store)
    # -- First snapshot -------------------------------------------------------
    df = cached_store.commit(df=dataset)
    assert df.shape == (2, 3)
    assert cached_store._cache is not None
    assert cached_store._cache.df.shape == (2, 3)
    assert cached_store._cache.version == 0
    assert cached_store.last_version() == 0
    df = cached_store.checkout()
    assert df.shape == (2, 3)
    df = df[df['A'] == 1]
    # -- Second snapshot ------------------------------------------------------
    df = cached_store.commit(df=df)
    assert df.shape == (1, 3)
    assert cached_store._cache.df.shape == (1, 3)
    assert cached_store._cache.version == 1
    assert len(cached_store.snapshots()) == 2
    # -- Checkout first snapshot ----------------------------------------------
    df = cached_store.checkout(version=0)
    assert df.shape == (2, 3)
    assert cached_store._cache.df.shape == (2, 3)
    assert cached_store._cache.version == 0
