# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for (de-)serialization of function handles."""

from openclean.engine.object.function import FunctionHandle, FunctionFactory
from openclean.engine.object.function import Int


def my_func(n):
    import time
    time.sleep(n)


def test_serialize_function():
    """Test serialization and deserialization of function handles."""
    # Serialize minimal function handle.
    f = FunctionHandle(func=my_func)
    doc, data = FunctionFactory().serialize(f)
    f = FunctionFactory().deserialize(descriptor=doc, data=data)
    assert f.name == 'my_func'
    assert f.namespace is None
    assert f.label is None
    assert f.description is None
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
        description='Just a test',
        columns=2,
        outputs=3,
        parameters=[Int('sleep')]
    )
    doc, data = FunctionFactory().serialize(f)
    f = FunctionFactory().deserialize(descriptor=doc, data=data)
    assert f.name == 'myname'
    assert f.namespace == 'mynamespace'
    assert f.label == 'My Name'
    assert f.description == 'Just a test'
    assert f.columns == 2
    assert f.outputs == 3
    assert len(f.parameters) == 1
    assert f.parameters[0].is_int()
    f.func(0.1)
