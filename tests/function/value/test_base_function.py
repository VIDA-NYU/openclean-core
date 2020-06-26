# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for constructors of base functions."""

from openclean.function.value.base import CallableWrapper


def test_callable_wrapper_function():
    """Test initializing the callable wrapper function."""
    # Use default funciton name.
    f = CallableWrapper(len)
    assert f.name == 'len'
    assert f.eval('abc') == 3
    # Specify custom function name.
    f = CallableWrapper(len, name='strlen')
    assert f.name == 'strlen'
    assert f.eval('abc') == 3
