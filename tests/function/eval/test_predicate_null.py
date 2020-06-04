# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for is empty predicates for data frame rows."""

from openclean.function.eval.predicate.null import IsEmpty, IsNotEmpty


def test_predicate_null(employees):
    """Test is empty and not is empty predicates."""
    # -- IsEmpty --------------------------------------------------------------
    f = IsEmpty(columns='Salary')
    f.prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
    assert not f.eval(employees.iloc[2])
    # NaN is not empty
    f = IsEmpty(columns='Age')
    f.prepare(employees)
    assert not f.eval(employees.iloc[3])
    # Apply on multiple columns
    f = IsEmpty(columns=['Age', 'Salary'], for_all=True)
    f.prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    f = IsEmpty(columns=['Age', 'Salary'], for_all=False)
    f.prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
    # -- IsNotEmpty -----------------------------------------------------------
    f = IsNotEmpty(columns='Salary')
    f.prepare(employees)
    assert f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    assert f.eval(employees.iloc[2])
    # NaN is not empty
    f = IsNotEmpty(columns='Age')
    f.prepare(employees)
    assert f.eval(employees.iloc[3])
    # Apply on multiple columns
    f = IsNotEmpty(columns=['Age', 'Salary'], for_all=True)
    f.prepare(employees)
    assert f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    f = IsNotEmpty(columns=['Age', 'Salary'], for_all=False)
    f.prepare(employees)
    assert f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
