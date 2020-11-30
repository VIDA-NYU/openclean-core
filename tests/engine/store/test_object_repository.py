# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the default implementation of the object repository."""

import pytest

from openclean.engine.store.fs import FileSystemObjectStore
from openclean.engine.store.mem import VolatileObjectRepository
from openclean.engine.store.serialized import SerializedObjectRepository


def fsrepo(basedir):
    """Get default serialized object repository using a file system store."""
    return SerializedObjectRepository(store=FileSystemObjectStore(basedir))


def memrepo(basedir):
    """Get volatile object repository."""
    return VolatileObjectRepository()


@pytest.mark.parametrize('repo', [fsrepo, memrepo])
def test_data_type_listings(repo, tmpdir):
    """Test listing the namespaces and data types for stored objects."""
    store = repo(tmpdir)
    # Initially the list of namespaces and data types is empty.
    assert store.types() == set()
    # Store several objects with two different name spaces and three types.
    doc = {'A': 1}
    store.insert_object(doc, name='o1', dtype='t1')
    store.insert_object(doc, name='o1', namespace='n1', dtype='t2')
    store.insert_object(doc, name='o2', namespace='n1', dtype='t3')
    store.insert_object(doc, name='o3', namespace='n2', dtype='t3')
    store.insert_object(doc, name='o4', namespace='n2', dtype='t3')
    assert store.types() == {'t1', 't2', 't3'}
    # Delete objects for one namespace and one data type.
    store.remove_object(name='o1', namespace='n1')
    store.remove_object(name='o2', namespace='n1')
    assert store.types() == {'t1', 't3'}


@pytest.mark.parametrize('repo', [fsrepo, memrepo])
def test_find_objects(repo, tmpdir):
    """Test object queries."""
    store = repo(tmpdir)
    store.insert_object({'A': 1}, name='o1', dtype='t1')
    store.insert_object({'B': 2}, name='o1', namespace='n1', dtype='t2')
    assert {obj.namespace: obj.name for obj in store.find_objects()} == {None: 'o1', 'n1': 'o1'}
    assert {obj.namespace: obj.name for obj in store.find_objects(dtype='t2')} == {'n1': 'o1'}
    assert {obj.namespace: obj.name for obj in store.find_objects(namespace='n1')} == {'n1': 'o1'}
    assert {obj.namespace: obj.name for obj in store.find_objects(namespace='n1', dtype='t2')} == {'n1': 'o1'}


@pytest.mark.parametrize('repo', [fsrepo, memrepo])
def test_object_lifecycle(repo, tmpdir):
    """Test the object life-cycle."""
    store = repo(tmpdir)
    store.insert_object({'A': 1}, name='o1', dtype='t1')
    store.insert_object({'B': 2}, name='o1', namespace='n1', dtype='t2')
    assert store.get_object(name='o1') == {'A': 1}
    assert store.get_object(name='o1', namespace='n1') == {'B': 2}
    store.remove_object(name='o1', namespace='n1')
    assert store.get_object(name='o1') == {'A': 1}
    with pytest.raises(KeyError):
        store.get_object(name='o1', namespace='n1')
    with pytest.raises(KeyError):
        store.remove_object(name='o1', namespace='n1')
