# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for different data store implementations."""

from datetime import datetime

import os
import pytest

from openclean.data.store.fs import FileSystemJsonStore
from openclean.data.store.mem import VolatileDataStore


def test_json_object_store(tmpdir):
    """Test object life cycle for volatile object store."""
    basedir = os.path.join(tmpdir, 'store')
    store = FileSystemJsonStore(basedir=basedir)
    # Create and read a new object.
    doc = {'A': 'a', 'B': 1, 'C': datetime.now()}
    object_id = store.write_object(doc)
    assert store.read_object(object_id) == doc
    # Replace the object.
    doc = {'A': 'x', 'C': datetime.now()}
    object_id = store.write_object(doc, object_id=object_id)
    # Recreate object store. Read the object to ensure that the new version
    # was materialized. Then delete the object.
    store = FileSystemJsonStore(basedir=basedir)
    assert store.read_object(object_id) == doc
    store.delete_object(object_id)
    # Errors when accessing a deleted object.
    with pytest.raises(KeyError):
        store.read_object(object_id)
    with pytest.raises(KeyError):
        store.delete_object(object_id)
    store = FileSystemJsonStore(basedir=basedir)
    with pytest.raises(KeyError):
        store.read_object(object_id)


def test_volatile_object_store():
    """Test object life cycle for volatile object store."""
    store = VolatileDataStore()
    # Create a new object and read it.
    doc = {'A': 'a', 'B': 1, 'C': datetime.now()}
    object_id = store.write_object(doc)
    assert store.read_object(object_id) == doc
    # Replace an existing object.
    doc = {'A': 'x', 'C': datetime.now()}
    object_id = store.write_object(doc)
    assert store.read_object(object_id) == doc
    # Delete object. Any following attempt to access to the deleted object will
    # raise a KeyError.
    store.delete_object(object_id)
    with pytest.raises(KeyError):
        store.read_object(object_id)
    with pytest.raises(KeyError):
        store.delete_object(object_id)
