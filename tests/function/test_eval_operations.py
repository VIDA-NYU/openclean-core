# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for binary comparison and arithmetic operations on evaluation
function results.
"""

from openclean.function.eval.base import Const


# -- Arithmetic operators -----------------------------------------------------

def test_add_eval_functions():
    """Test computing the sum of evaluation function values."""
    f = Const(2) + Const(3)
    assert f.eval([]) == 5
    f = Const(2) + Const(3) + Const(4)
    assert f.eval([]) == 9


def test_divide_eval_functions():
    """Test computing the division of evaluation function values."""
    f = Const(7) / Const(3)
    assert f.eval([]) == 7/3
    f = Const(7) // Const(3)
    assert f.eval([]) == 2


def test_multiply_eval_functions():
    """Test computing the product of evaluation function values."""
    f = Const(2) * Const(3)
    assert f.eval([]) == 6


def test_subtract_eval_functions():
    """Test computing the subtraction of evaluation function values."""
    f = Const(2) - Const(3)
    assert f.eval([]) == -1


# -- Comparison operators -----------------------------------------------------

def test_eq_comparison():
    """Test comparing two evaluation functions for equality."""
    f = Const(2) == Const(2)
    assert f.eval([])
    f = Const(2) == Const(3)
    assert not f.eval([])


def test_neq_comparison():
    """Test comparing two evaluation functions for inequality."""
    f = Const(2) != Const(2)
    assert not f.eval([])
    f = Const(2) != Const(3)
    assert f.eval([])


def test_geq_comparison():
    """Test comparing two evaluation functions using '>='."""
    f = Const(2) >= Const(2)
    assert f.eval([])
    f = Const(2) >= Const(3)
    assert not f.eval([])
    f = Const(3) >= Const(2)
    assert f.eval([])


def test_gt_comparison():
    """Test comparing two evaluation functions using '>'."""
    f = Const(2) > Const(2)
    assert not f.eval([])
    f = Const(2) > Const(3)
    assert not f.eval([])
    f = Const(3) > Const(2)
    assert f.eval([])


def test_leq_comparison():
    """Test comparing two evaluation functions using '<='."""
    f = Const(2) <= Const(2)
    assert f.eval([])
    f = Const(2) <= Const(3)
    assert f.eval([])
    f = Const(3) <= Const(2)
    assert not f.eval([])


def test_lt_comparison():
    """Test comparing two evaluation functions using '<'."""
    f = Const(2) < Const(2)
    assert not f.eval([])
    f = Const(2) < Const(3)
    assert f.eval([])
    f = Const(3) < Const(2)
    assert not f.eval([])
