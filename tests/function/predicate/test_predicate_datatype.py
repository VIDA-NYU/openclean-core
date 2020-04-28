# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for data type detection predicates."""

from collections import Counter

from openclean.function.predicate.datatype import IsDate, IsInt, IsFloat, IsNaN


def test_predicate_datatype(employees):
    """Test data type detection predicates."""
    predicates = [
        IsDate(format='%m/%d/%y', columns='Name'),
        IsInt('Salary'),
        IsFloat('Salary'),
        IsNaN('Age')
    ]
    for f in predicates:
        f.prepare(employees)
    counts = Counter()
    for rowid, values in employees.iterrows():
        for i in range(len(predicates)):
            if predicates[i].exec(values):
                counts[i] += 1
    assert counts[0] == 0
    assert counts[1] == 3
    assert counts[2] == 3
    assert counts[3] == 1
    # Multiple columns
    f = IsInt(['Age', 'Salary'], for_all=True)
    f.prepare(employees)
    count = 0
    for rowid, values in employees.iterrows():
        if f(values):
            count += 1
    assert count == 0
    f = IsInt(['Age', 'Salary'], for_all=False)
    f.prepare(employees)
    count = 0
    for rowid, values in employees.iterrows():
        if f(values):
            count += 1
    assert count == 3
    f = IsFloat(['Age', 'Salary'], for_all=True)
    f.prepare(employees)
    count = 0
    for rowid, values in employees.iterrows():
        if f(values):
            count += 1
    assert count == 3
    f = IsFloat(['Age', 'Salary'], for_all=False)
    f.prepare(employees)
    count = 0
    for rowid, values in employees.iterrows():
        if f(values):
            count += 1
    assert count == 7
