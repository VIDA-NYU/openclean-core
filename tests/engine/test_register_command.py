# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for registering and executing commands via the openclean engine."""


def add_ten(n):
    """Test update function that adds ten to a given argument."""
    return n + 10


def test_register_and__update(volatile_engine):
    """Test registering and applying a user defined function."""
    volatile_engine.register.eval('addten')(add_ten)
    f = volatile_engine.register.get('addten')
    assert f(1) == 11
    assert 'addten' in {obj['name'] for obj in volatile_engine.register.serialize()}
