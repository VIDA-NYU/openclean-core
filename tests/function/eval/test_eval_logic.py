# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for predicate logic operators."""

import pandas as pd
import pytest

from openclean.function.eval.base import Col
from openclean.function.eval.logic import And, Not, Or


@pytest.fixture
def dataset():
    """Simple dataset with the name and age of four people."""
    return pd.DataFrame(
        data=[
            ['Alice', 45],
            ['Bob', 23],
            ['Claudia', 25],
            ['Dave', 56]
        ],
        columns=['Name', 'Age']
    )


def test_predicate_logic_and(dataset):
    """Test functionality of logic AND operator."""
    op = And(Col('Name') > 'Bob', Col('Age') > 30)
    result = op.eval(dataset)
    assert result == [False, False, False, True]
    f = op.prepare(dataset.columns)
    result = [f(t) for t in dataset.itertuples(index=False, name=None)]
    assert result == [False, False, False, True]


def test_predicate_logic_Not(dataset):
    """Test functionality of logic NOT operator."""
    op = Not(Col('Name') == 'Alice')
    result = op.eval(dataset)
    assert result == [False, True, True, True]
    f = op.prepare(dataset.columns)
    result = [f(t) for t in dataset.itertuples(index=False, name=None)]
    assert result == [False, True, True, True]


def test_predicate_logic_or(dataset):
    """Test functionality of logic OR operator."""
    op = Or(Col('Name') > 'Bob', Col('Age') > 30)
    result = op.eval(dataset)
    assert result == [True, False, True, True]
    f = op.prepare(dataset.columns)
    result = [f(t) for t in dataset.itertuples(index=False, name=None)]
    assert result == [True, False, True, True]
