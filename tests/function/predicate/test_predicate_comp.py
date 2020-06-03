# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the comparison predicates."""

import pandas as pd

from openclean.function.column import Col
from openclean.function.predicate.comp import (
    Eq, EqIgnoreCase, Geq, Gt, Leq, Lt, Neq
)


def test_predicate_eq(employees):
    """Test equality operators."""
    # -- Eq and EqIgnoreCase --------------------------------------------------
    f = Eq(Col('Name'), 'Alice').prepare(employees)
    assert f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    f = Eq(Col('Name'), 'alice').prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    f = EqIgnoreCase(Col('Name'), 'alice').prepare(employees)
    assert f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    # -- Neq ------------------------------------------------------------------
    f = Neq(Col('Name'), 'Alice').prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])


def test_predicate_greater(employees):
    """Test the greater than and greater or equal predicates."""
    f = Gt(Col('Age'), 32).prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    assert f.eval(employees.iloc[2])
    f = Geq(Col('Age'), 32).prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
    assert f.eval(employees.iloc[2])


def test_predicate_lower(employees):
    """Test the lower than and lower or equal predicates."""
    f = Lt(Col('Age'), 32).prepare(employees)
    assert f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    assert not f.eval(employees.iloc[2])
    f = Leq(Col('Age'), 32).prepare(employees)
    assert f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
    assert not f.eval(employees.iloc[2])


def test_predicate_with_two_expressions():
    """Test operators with two evaluation functions."""
    df = pd.DataFrame(data=[[1, 2], [3, 3]], columns=['A', 'B'])
    f1 = Eq(Col('A'), Col('B')).prepare(df)
    f2 = Gt(Col('B'), Col('A')).prepare(df)
    assert not f1.eval(df.iloc[0])
    assert f1.eval(df.iloc[1])
    assert f2.eval(df.iloc[0])
    assert not f2.eval(df.iloc[1])
