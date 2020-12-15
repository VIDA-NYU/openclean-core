# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the function handle object store. Uses a persistent data
store to test serialization and deserialization of functions that are store in
files on disk.
"""

import pytest

from openclean.engine.object.function import FunctionHandle
from openclean.engine.store.function import FunctionRepository


def add_one(value):
    """Simple add function for test purposes."""
    import time
    time.sleep(0.1)
    return value + 1


def test_volatile_object_store(tmpdir):
    """Test all methods of the volatile object store."""
    store = FunctionRepository(
        basedir=tmpdir,
        defaults=[
            FunctionHandle(func=str.lower),
            FunctionHandle(func=str.upper)
        ]
    )
    # Register a new function.
    store.insert_object(FunctionHandle(name='myfunc', func=add_one))
    # Validate that all functions work as expected.
    assert store.get_object(name='lower')('ABC') == 'abc'
    assert store.get_object(name='upper')('abc') == 'ABC'
    assert store.get_object(name='myfunc')(1) == 2
    # Recreate the store without defaults. Then ensure that functions are still
    # working as expected. There should be no need to re-register the add_one
    # function.
    store = FunctionRepository(
        basedir=tmpdir,
        defaults=[
            FunctionHandle(func=str.lower),
            FunctionHandle(func=str.upper)
        ]
    )
    assert store.get_object(name='lower')('ABC') == 'abc'
    assert store.get_object(name='upper')('abc') == 'ABC'
    assert store.get_object(name='myfunc')(1) == 2
    # Create store without defaults. Only add_one will work.
    store = FunctionRepository(basedir=tmpdir)
    with pytest.raises(KeyError):
        store.get_object(name='lower')
    with pytest.raises(KeyError):
        store.get_object(name='upper')
    assert store.get_object(name='myfunc')(1) == 2
