# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for is empty predicates for data frame rows."""

from openclean.function.eval.base import Col
from openclean.function.eval.null import IsEmpty, IsNotEmpty


def test_predicate_isempty(employees):
    """Test is empty predicate."""
    f = IsEmpty(columns='Salary').prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
    assert not f.eval(employees.iloc[2])
    # NaN is not empty
    f = IsEmpty(columns=Col('Age')).prepare(employees)
    assert not f.eval(employees.iloc[3])


def test_predicate_isnotempty(employees):
    """Test the IsNotEmpty predicate."""
    f = IsNotEmpty(columns='Salary').prepare(employees)
    assert f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    assert f.eval(employees.iloc[2])
    # NaN is not empty
    f = IsNotEmpty(columns='Age').prepare(employees)
    assert f.eval(employees.iloc[3])
