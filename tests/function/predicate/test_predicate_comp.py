# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the comparison predicates."""

from openclean.function.predicate.comp import (
    Eq, EqIgnoreCase, Geq, Gt, Leq, Lt, Neq
)


def test_predicate_eq(employees):
    """Test equality operators."""
    # -- Eq and EqIgnoreCase --------------------------------------------------
    f = Eq('Name', 'Alice')
    f.prepare(employees)
    assert f.exec(employees.iloc[0])
    assert not f.exec(employees.iloc[1])
    f = Eq('Name', 'alice')
    f.prepare(employees)
    assert not f.exec(employees.iloc[0])
    assert not f.exec(employees.iloc[1])
    f = EqIgnoreCase('Name', 'alice')
    f.prepare(employees)
    assert f.exec(employees.iloc[0])
    assert not f.exec(employees.iloc[1])
    # -- Neq ------------------------------------------------------------------
    f = Neq('Name', 'Alice')
    f.prepare(employees)
    assert not f.exec(employees.iloc[0])
    assert f.exec(employees.iloc[1])


def test_predicate_greater(employees):
    """Test the greater than and greater or equal predicates."""
    f = Gt('Age', 32)
    f.prepare(employees)
    assert not f.exec(employees.iloc[0])
    assert not f.exec(employees.iloc[1])
    assert f.exec(employees.iloc[2])
    f = Geq('Age', 32)
    f.prepare(employees)
    assert not f.exec(employees.iloc[0])
    assert f.exec(employees.iloc[1])
    assert f.exec(employees.iloc[2])


def test_predicate_lower(employees):
    """Test the lower than and lower or equal predicates."""
    f = Lt('Age', 32)
    f.prepare(employees)
    assert f.exec(employees.iloc[0])
    assert not f.exec(employees.iloc[1])
    assert not f.exec(employees.iloc[2])
    f = Leq('Age', 32)
    f.prepare(employees)
    assert f.exec(employees.iloc[0])
    assert f.exec(employees.iloc[1])
    assert not f.exec(employees.iloc[2])
