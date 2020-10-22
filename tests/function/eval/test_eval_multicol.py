# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the implementations of basic evaluation functions when using
multiple columns.
"""

import pandas as pd
import pytest

from openclean.function.eval.base import Cols


@pytest.fixture
def dataset():
    """Use a simple data frame for all tests. The data frame has the following
    structure:

    A B A
    -----
    1 2 3
    1 2 3
    3 4 5
    5 6 7
    7 8 9
    """
    return pd.DataFrame(
        data=[
            [0, 0, 0],
            [1, 2, 3],
            [5, 4, 3],
            [5, 6, 7],
            [9, 8, 7]
        ],
        columns=['A', 'B', 'A']
    )


# -- Comparison operators -----------------------------------------------------

def test_eq_cols(dataset):
    """Test equality between tuples from multiple columns."""
    op = Cols([0, 1]) == Cols([1, 2])
    assert op.eval(dataset) == [True] + [False] * 4


def test_leq_cols(dataset):
    """Test lower or equal operator for tuples from different columns."""
    op = Cols([0, 1]) <= Cols([1, 2])
    assert op.eval(dataset) == [True, True, False, True, False]

def test_neq_cols(dataset):
    """Test in-equality between tuples from multiple columns."""
    op = Cols([0, 1]) != Cols([1, 2])
    assert op.eval(dataset) == [False] + [True] * 4


# -- Arithmetic operators -----------------------------------------------------

def test_add_cols(dataset):
    """Test adding tuples from different columns of a dataset."""
    op = Cols([0, 1]) + Cols([1, 2])
    assert op.eval(dataset)[:3] == [(0, 0, 0, 0), (1, 2, 2, 3), (5, 4, 4, 3)]

def test_sub_cols(dataset):
    """Test type error for arithmetic operations on tuples that are not
    supported.
    """
    op = Cols([0, 1]) - Cols([1, 2])
    with pytest.raises(TypeError):
        op.eval(dataset)
