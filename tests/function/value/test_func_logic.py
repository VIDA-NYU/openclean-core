# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for logic functions."""

import pytest

from openclean.function.value.domain import IsInDomain
from openclean.function.value.logic import And, Not, Or
from openclean.function.value.null import is_empty


DOM1 = IsInDomain(['A', 'B'])
DOM2 = IsInDomain(['B', 'C'])


def test_func_and():
    """Test conjunction."""
    f = And(DOM1, DOM2)
    assert not f('A')
    assert f('B')
    assert not f('C')
    assert not f('D')
    f = And(DOM1, [DOM2])
    assert not f('A')
    assert f('B')
    # Erorr case for invalid argument list.
    with pytest.raises(ValueError):
        And(DOM1, [DOM2, 'A'])


def test_func_not():
    """Test negation."""
    f = Not(is_empty)
    assert not f('')
    assert f('A')
    # Error cases.
    with pytest.raises(ValueError):
        Not(is_empty, DOM1)
    with pytest.raises(ValueError):
        Not('A')


def test_func_or():
    """Test disjunction."""
    f = Or([DOM1, DOM2])
    assert f('A')
    assert f('B')
    assert f('C')
    assert not f('D')
