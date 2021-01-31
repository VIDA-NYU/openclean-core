# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for (de-)serialization of mapping object handles."""

from openclean.data.mapping import Mapping, StringMatch
from openclean.engine.object.mapping import MappingHandle, MappingFactory


# Global mapping for test purposes.
MAPPING = Mapping().add('A', [StringMatch(term='B', score=0.5)])


def test_serialize_mapping():
    """Test serialization and deserialization of mapping handles."""
    # Serialize minimal mapping handle.
    v = MappingHandle(mapping=MAPPING, name='my_lookup')
    doc, data = MappingFactory().serialize(v)
    vocab = MappingFactory().deserialize(descriptor=doc, data=data)
    assert vocab.name == 'my_lookup'
    assert vocab.namespace is None
    assert vocab.label is None
    assert vocab.description is None
    assert vocab.mapping == MAPPING
    # Serialize maximal vocabulary handle.
    v = MappingHandle(
        mapping=MAPPING,
        name='my_lookup',
        namespace='mynamespace',
        label='My Name',
        description='Just a test'
    )
    doc, data = MappingFactory().serialize(v)
    vocab = MappingFactory().deserialize(descriptor=doc, data=data)
    assert vocab.name == 'my_lookup'
    assert vocab.namespace == 'mynamespace'
    assert vocab.label == 'My Name'
    assert vocab.description == 'Just a test'
    assert vocab.mapping == MAPPING
