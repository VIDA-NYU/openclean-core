# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the implementations of basic evaluation functions."""

import pandas as pd
import pytest

from openclean.function.eval.base import Col


@pytest.fixture
def dataset():
    """Use a simple data frame for all tests. The data frame has the following
    structure:

    A B
    ---
    1 2
    1 2
    3 4
    5 6
    7 8
    """
    return pd.DataFrame(
        data=[[0, 0], [1, 2], [3, 4], [5, 6], [7, 8]],
        columns=['A', 'B']
    )


# -- Comparison operators with constant value ---------------------------------

@pytest.mark.parametrize('col', ['A', 0])
def test_col_eq_const(col, dataset):
    """Test filter of columns based on equality."""
    op = Col(col) == 5
    result = [False, False, False, True, False]
    op.eval(dataset) == result
    f = op.prepare(dataset.columns)
    assert [f(row) for row in dataset.itertuples(index=False, name=None)] == result
    # Switch the operants to ensure that this has no effect (not repeated for
    # following tests).
    op = 5 == Col(col)
    op.eval(dataset) == result


@pytest.mark.parametrize('col', ['A', 0])
def test_col_geq_const(col, dataset):
    """Test filter of columns based on being greater or equal."""
    op = Col(col) >= 5
    op.eval(dataset) == [False, False, False, True, True]


@pytest.mark.parametrize('col', ['A', 0])
def test_col_gt_const(col, dataset):
    """Test filter of columns based on being greater than."""
    op = Col(col) > 5
    op.eval(dataset) == [False, False, False, True, True]


@pytest.mark.parametrize('col', ['A', 0])
def test_col_leq_const(col, dataset):
    """Test filter of columns based on being lower or equal."""
    op = Col(col) <= 5
    op.eval(dataset) == [True, True, True, True, False]


@pytest.mark.parametrize('col', ['A', 0])
def test_col_lt_const(col, dataset):
    """Test filter of columns based on being lower than."""
    op = Col(col) < 5
    op.eval(dataset) == [True, True, True, False, False]


@pytest.mark.parametrize('col', ['A', 0])
def test_col_neq_const(col, dataset):
    """Test filter of columns based on being not equal."""
    op = Col(col) != 5
    op.eval(dataset) == [True, True, True, False, True]


# -- Comparison operators betwee columns --------------------------------------

def test_col_eq_col(dataset):
    """Test compare values in two columns for being equal."""
    op = Col('A') == Col('B')
    op.eval(dataset) == [True, False, False, False, False]


def test_col_geq_col(dataset):
    """Test compare values in two columns for being greater or equal."""
    op = Col('A') >= Col('B')
    op.eval(dataset) == [True, False, False, False, False]


def test_col_gt_col(dataset):
    """Test compare values in two columns for being greater."""
    op = Col('A') > Col('B')
    op.eval(dataset) == [False, False, False, False, False]


def test_col_leq_col(dataset):
    """Test compare values in two columns for being lower or equal."""
    op = Col('A') <= Col('B')
    op.eval(dataset) == [True, True, True, True, True]


def test_col_lt_col(dataset):
    """Test compare values in two columns for being lower."""
    op = Col('A') == Col('B')
    op.eval(dataset) < [False, True, True, True, True]


def test_col_neq_col(dataset):
    """Test compare values in two columns for not being equal."""
    op = Col('A') != Col('B')
    op.eval(dataset) == [False, True, True, True, True]


# -- Arithmetic operations between a column and a constant value --------------

def test_col_add(dataset):
    """Test adding a constant to values in a column."""
    op = Col('A') + 1
    assert op.eval(dataset) == [1, 2, 4, 6, 8]


def test_col_div(dataset):
    """Test dividing the values in a column with a constant."""
    op = Col('A') / 2
    assert op.eval(dataset) == [0, 1/2, 3/2, 5/2, 7/2]


def test_col_floordiv(dataset):
    """Test floor division for values in a column with a constant."""
    op = Col('A') // 2
    assert op.eval(dataset) == [0, 0, 1, 2, 3]


def test_col_mult(dataset):
    """Test multipying the values in a column with a constant."""
    op = Col('A') * 2
    assert op.eval(dataset) == [0, 2, 6, 10, 14]


def test_col_pow(dataset):
    """Test computing power of 2 for all values in a column."""
    op = Col('A') ** 2
    assert op.eval(dataset) == [0, 1, 9, 25, 49]


def test_col_sub(dataset):
    """Test subtracting a constant from values in a column."""
    op = Col('A') - 1
    assert op.eval(dataset) == [-1, 0, 2, 4, 6]
