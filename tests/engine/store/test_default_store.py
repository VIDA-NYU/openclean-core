# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the default object store using a volatile data store and
the controlled vocabulary object type.
"""

import pytest

from openclean.data.store.mem import VolatileDataStore
from openclean.engine.object.vocabulary import VocabularyFactory, VocabularyHandle
from openclean.engine.store.default import DefaultObjectStore


@pytest.fixture
def store():
    """Get an instance of the default object store."""
    return DefaultObjectStore(
        identifier='test',
        factory=VocabularyFactory(),
        store=VolatileDataStore()
    )


def test_object_life_cycle(store):
    """Test storing, retrieving and deleting objects."""
    store.insert_object(VocabularyHandle(name='o1', values=['A', 'B']))
    store.insert_object(VocabularyHandle(name='o1', namespace='n1', values=['C']))
    # Get the objects an ensure that the vocabularies are correct.
    assert store.get_object(name='o1').values == {'A', 'B'}
    assert store.get_object(name='o1', namespace='n1').values == {'C'}
    # Delete the first object. Accessing that object will raise an error. The
    # second object is still accesible.
    store.delete_object(name='o1')
    assert store.get_object(name='o1', namespace='n1').values == {'C'}
    with pytest.raises(KeyError):
        store.get_object(name='o1')
    with pytest.raises(KeyError):
        store.delete_object(name='o1')
    # Delete the second object will leave us with an empty descriptor listing.
    store.delete_object(name='o1', namespace='n1')
    with pytest.raises(KeyError):
        store.get_object(name='o1', namespace='n1')
    assert len(store.to_listing()) == 0


def test_object_listings(store):
    """Test listing the descriptors for stored objects."""
    # Store several objects with two different name spaces.
    objects = [
        VocabularyHandle(name='o1', values=['A', 'B']),
        VocabularyHandle(name='o1', namespace='n1', values=['C']),
        VocabularyHandle(name='o2', namespace='n1', values=['D'])
    ]
    for i in range(len(objects)):
        assert len(store.to_listing()) == i
        store.insert_object(objects[i])
    assert len(store.to_listing()) == len(objects)
    for obj in store.to_listing():
        assert isinstance(obj, dict)
    # Adding an object with an existing identifier won't change the overall
    # number of entries in the object listing.
    for i in range(len(objects)):
        store.insert_object(objects[i])
        assert len(store.to_listing()) == len(objects)


def test_object_store_with_defaults():
    """Test providing a set of default objects when initializing the object
    store.
    """
    store = DefaultObjectStore(
        identifier='test',
        factory=VocabularyFactory(),
        store=VolatileDataStore(),
        defaults=[
            VocabularyHandle(name='o1', values=['A', 'B']),
            VocabularyHandle(name='o1', namespace='n1', values=['C'])
        ]
    )
    store.insert_object(VocabularyHandle(name='o2', namespace='n1', values=['D']))
    assert len(store.to_listing()) == 3
    assert store.get_object(name='o1').values == ['A', 'B']
    assert store.get_object(name='o1', namespace='n1').values == ['C']
    assert store.get_object(name='o2', namespace='n1').values == {'D'}
    # Override a default object.
    store.insert_object(VocabularyHandle(name='o1', namespace='n1', values=['E']))
    assert len(store.to_listing()) == 3
    assert store.get_object(name='o1', namespace='n1').values == {'E'}
    # Delete an overwritten default object.
    store.delete_object(name='o1', namespace='n1')
    with pytest.raises(KeyError):
        store.get_object(name='o1', namespace='n1')
    assert store.get_object(name='o2', namespace='n1').values == {'D'}
    # Delete a default object.
    store.delete_object(name='o1')
    with pytest.raises(KeyError):
        store.get_object(name='o1')
    assert store.get_object(name='o2', namespace='n1').values == {'D'}
    assert len(store.to_listing()) == 1
