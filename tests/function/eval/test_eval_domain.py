# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for domain predicates for data frame rows."""

from openclean.function.eval.base import Cols
from openclean.function.eval.domain import IsIn, IsNotIn


def test_predicate_domain(employees):
    """Test is in domain predicate."""
    f = IsIn(domain=['Alice', 'Bob'], columns='Name').prepare(employees)
    assert f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
    assert not f.eval(employees.iloc[2])
    # Tuple lookup over multiple columns
    f = IsIn(
        domain=[('Alice', '23'), ('Bob', 32.0)],
        columns=Cols('Name', 'Age')
    )
    f = f.prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
    assert not f.eval(employees.iloc[2])


def test_predicate_notin_domain(employees):
    """test the is not in domain predicate."""
    # -- IsNotIn --------------------------------------------------------------
    f = IsNotIn(domain=['Alice', 'Bob'], columns='Name').prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    assert f.eval(employees.iloc[2])
    # Tuple lookup over multiple columns
    f = IsNotIn(
        domain=[('Alice', '23'), ('Bob', 32.0)],
        columns=['Name', 'Age']
    )
    f = f.prepare(employees)
    assert f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    assert f.eval(employees.iloc[2])
