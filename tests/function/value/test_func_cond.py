# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the if-then-else statement."""

from openclean.function.value.comp import Eq
from openclean.function.value.cond import IfThenElse, IfThen, Replace


def test_if_the_else():
    """Test the if-then-else statement."""
    f = IfThenElse(Eq(2), 4, lambda x: x*x)
    assert f(2) == 4
    assert f(3) == 9
    assert f(4) == 16


def test_if_then_synonyms():
    """Test the synonym functions for if-then statements."""
    f = IfThen(Eq(2), 4)
    assert f(2) == 4
    assert f(3) == 3
    assert f(4) == 4
    f = Replace(Eq(2), 4)
    assert f(2) == 4
    assert f(3) == 3
    assert f(4) == 4
