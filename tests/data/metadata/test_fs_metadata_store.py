# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the file system metadata store."""

from openclean.data.metadata.fs import FileSystemMetadataStoreFactory

import histore.util as util


def test_delete_annotations(tmpdir):
    """Test deleting annotations for different types of resources."""
    # -- Setup ----------------------------------------------------------------
    store = FileSystemMetadataStoreFactory(basedir=str(tmpdir)).get_store(0)
    store.set_annotation(key='A', value=1)
    store.set_annotation(key='B', value=2, column_id=1)
    store.set_annotation(key='A', value=3, row_id=2)
    store.set_annotation(key='A', value=4, column_id=1, row_id=2)
    # -- Delete column annotation ---------------------------------------------
    assert store.has_annotation(key='B', column_id=1)
    assert store.get_annotation(key='B', column_id=1) == 2
    assert store.list_annotations(column_id=1) == dict({'B': 2})
    store.delete_annotation(key='B', column_id=1)
    assert not store.has_annotation(key='B', column_id=1)
    assert store.get_annotation(key='B', column_id=1) is None
    assert store.list_annotations(column_id=1) == dict()
    store.delete_annotation(key='A', column_id=1)
    store.delete_annotation(key='B', column_id=1)
    assert store.get_annotation(key='A', column_id=1) is None
    assert store.get_annotation(key='B', column_id=1) is None
    # -- Delete row annotation ------------------------------------------------
    assert store.get_annotation(key='A', row_id=2) == 3
    store.delete_annotation(key='A', row_id=2)
    assert store.get_annotation(key='A', row_id=2) is None
    # -- Delete cell annotation -----------------------------------------------
    assert store.get_annotation(key='A', column_id=1, row_id=2) == 4
    store.delete_annotation(key='A', column_id=1, row_id=2)
    assert store.get_annotation(key='A', column_id=1, row_id=2) is None
    # -- Delete dataset annotation --------------------------------------------
    assert store.get_annotation(key='A') == 1
    store.delete_annotation(key='A')
    assert store.get_annotation(key='A') is None


def test_update_annotations(tmpdir):
    """Test creating and updating annotations."""
    # -- Setup ----------------------------------------------------------------
    store = FileSystemMetadataStoreFactory(basedir=str(tmpdir)).get_store(0)
    # -- Create and update dataset annotation ---------------------------------
    store.set_annotation(key='A', value=1)
    store.set_annotation(key='B', value=2)
    assert store.get_annotation(key='A') == 1
    assert store.get_annotation(key='B') == 2
    ts = util.utc_now()
    store.set_annotation(key='A', value=ts)
    assert store.get_annotation(key='A') == ts
    assert store.get_annotation(key='B') == 2
    # -- Create and update column annotation ----------------------------------
    store.set_annotation(key='A', column_id=1, value=ts)
    assert store.get_annotation(key='A', column_id=1) == ts
    store.set_annotation(key='A', column_id=1, value=10)
    assert store.get_annotation(key='A', column_id=1) == 10
