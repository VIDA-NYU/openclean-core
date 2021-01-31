# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for (de-)serialization of controlled vocabulary handles."""

from openclean.engine.object.vocabulary import VocabularyHandle, VocabularyFactory


def test_serialize_vocabulary():
    """Test serialization and deserialization of controlled vocabulary handles."""
    # Serialize minimal vocabulary handle.
    v = VocabularyHandle(values={'A', 'B', 'C'}, name='my_vocab')
    doc, data = VocabularyFactory().serialize(v)
    vocab = VocabularyFactory().deserialize(descriptor=doc, data=data)
    assert vocab.name == 'my_vocab'
    assert vocab.namespace is None
    assert vocab.label is None
    assert vocab.description is None
    assert vocab.values == {'A', 'B', 'C'}
    # Serialize maximal vocabulary handle.
    v = VocabularyHandle(
        values={'A', 'B', 'C'},
        name='my_vocab',
        namespace='mynamespace',
        label='My Name',
        description='Just a test'
    )
    doc, data = VocabularyFactory().serialize(v)
    vocab = VocabularyFactory().deserialize(descriptor=doc, data=data)
    assert vocab.name == 'my_vocab'
    assert vocab.namespace == 'mynamespace'
    assert vocab.label == 'My Name'
    assert vocab.description == 'Just a test'
    assert vocab.values == {'A', 'B', 'C'}
