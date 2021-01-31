# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for registering objects via the openclean library."""

from openclean.data.mapping import Mapping
from openclean.engine.library import ObjectLibrary


def add_ten(n):
    """Test update function that adds ten to a given argument."""
    return n + 10


def test_register_user_defined_function():
    """Test registering and applying a user defined function."""
    library = ObjectLibrary()
    f = library.eval('addten')(add_ten)
    assert f(1) == 11
    f = library.functions().get('addten')
    assert f(1) == 11


def test_register_lookup_table():
    """Test registering a lookup table with the library."""
    library = ObjectLibrary()
    handle = library.lookup(
        mapping=Mapping({'A': 'a', 'B': 'b'}),
        name='mymap',
        namespace='my-looks'
    )
    assert handle.mapping.to_lookup() == {'A': 'a', 'B': 'b'}
    handle = library.lookups().get(name='mymap', namespace='my-looks')
    assert handle.mapping.to_lookup() == {'A': 'a', 'B': 'b'}


def test_register_vocabulary():
    """Test registering a controlled vocabulary with the library."""
    library = ObjectLibrary()
    handle = library.vocabulary(
        values=['A', 'B', 'C'],
        name='myvoc',
        namespace='my-vocabs'
    )
    assert handle.values == {'A', 'B', 'C'}
    handle = library.vocabularies().get(name='myvoc', namespace='my-vocabs')
    assert handle.values == {'A', 'B', 'C'}
