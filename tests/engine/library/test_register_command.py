# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for registering and executing commands via the openclean library."""

from openclean.engine.library import ObjectLibrary


def add_ten(n):
    """Test update function that adds ten to a given argument."""
    return n + 10


def test_register_user_defined_function():
    """Test registering and applying a user defined function."""
    library = ObjectLibrary()
    f = library.eval('addten')(add_ten)
    assert f(1) == 11
    f = library.functions().get_object('addten')
    assert f(1) == 11
