# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for data type detection predicates."""

from collections import Counter

from openclean.function.eval.datatype import IsDatetime, IsInt, IsFloat, IsNaN


def test_predicate_datatype_for_single_column(employees):
    """Test data type detection predicates for values from a single column."""
    predicates = [
        IsDatetime(formats='%m/%d/%y', columns='Name').prepare(employees),
        IsInt('Salary').prepare(employees),
        IsFloat('Salary').prepare(employees),
        IsNaN('Age').prepare(employees)
    ]
    counts = Counter()
    for rowid, values in employees.iterrows():
        for i in range(len(predicates)):
            if predicates[i].eval(values):
                counts[i] += 1
    assert counts[0] == 0
    assert counts[1] == 3
    assert counts[2] == 3
    assert counts[3] == 1


def test_predicate_datatype_for_multi_column(employees):
    """Test data type detection predicates for values from multiple columns."""
    f = IsInt(['Age', 'Salary'], for_all=True).prepare(employees)
    count = 0
    for rowid, values in employees.iterrows():
        if f(values):
            count += 1
    assert count == 0
    f = IsInt(['Age', 'Salary'], for_all=False).prepare(employees)
    count = 0
    for rowid, values in employees.iterrows():
        if f(values):
            count += 1
    assert count == 3
    f = IsFloat(['Age', 'Salary'], for_all=True).prepare(employees)
    count = 0
    for rowid, values in employees.iterrows():
        if f(values):
            count += 1
    assert count == 3
    f = IsFloat(['Age', 'Salary'], for_all=False).prepare(employees)
    count = 0
    for rowid, values in employees.iterrows():
        if f(values):
            count += 1
    assert count == 7
