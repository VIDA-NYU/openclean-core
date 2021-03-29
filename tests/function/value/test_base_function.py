# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for constructors of base functions."""

from collections import Counter

import pytest

from openclean.function.value.base import CallableWrapper, ConstantValue, UnpreparedFunction


@pytest.mark.parametrize('threads', [1, 2])
def test_apply_on_counter(threads):
    """Test apply method for counter inputs."""
    values = Counter(['a', 'A', 'b', 'b'])
    f = CallableWrapper(func=str.upper)
    assert f.apply(values, threads=threads) == {'A': 2, 'B': 2}


@pytest.mark.parametrize('threads', [1, 2])
def test_apply_on_list(threads):
    """Test apply method for list inputs."""
    values = ['a', 'A', 'b', 'b']
    f = CallableWrapper(func=str.upper)
    assert Counter(f.apply(values, threads=threads)) == {'A': 2, 'B': 2}


def test_callable_wrapper_function():
    """Test initializing the callable wrapper function."""
    # Use default funciton name.
    f = CallableWrapper(len)
    assert f.eval('abc') == 3


def test_constant_value():
    """Test constant value function."""
    f = ConstantValue(value=2)
    assert f.eval('abc') == 2


def test_error_for_unprepared():
    """Ensure that an error is raised if eval is called for a unprepared
    value function.
    """

    class C(UnpreparedFunction):
        def prepare(self, values):
            return None

    f = C()
    with pytest.raises(NotImplementedError):
        f.eval(0)
