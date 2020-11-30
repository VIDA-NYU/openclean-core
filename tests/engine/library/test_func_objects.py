# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for (de-)serialization of function handles."""

from openclean.engine.library.func import FunctionHandle, FunctionSerializer
from openclean.engine.library.parameter import Int


def my_func(n):
    import time
    time.sleep(n)


def test_serialize_function():
    """Test serialization and deserialization of function handles."""
    # Serialize minimal function handle.
    f = FunctionHandle(func=my_func)
    doc = FunctionSerializer().serialize(f)
    f = FunctionSerializer().deserialize(doc)
    assert f.name == 'my_func'
    assert f.namespace is None
    assert f.label is None
    assert f.help is None
    assert f.columns == 1
    assert f.outputs == 1
    assert f.parameters == []
    f.func(0.1)
    # Serialize maximal function handle.
    f = FunctionHandle(
        func=my_func,
        name='myname',
        namespace='mynamespace',
        label='My Name',
        help='Just a test',
        columns=2,
        outputs=3,
        parameters=[Int('sleep')]
    )
    doc = FunctionSerializer().serialize(f)
    f = FunctionSerializer().deserialize(doc)
    assert f.name == 'myname'
    assert f.namespace == 'mynamespace'
    assert f.label == 'My Name'
    assert f.help == 'Just a test'
    assert f.columns == 2
    assert f.outputs == 3
    assert len(f.parameters) == 1
    assert f.parameters[0].is_int()
    f.func(0.1)
