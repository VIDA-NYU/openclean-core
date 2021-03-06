# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for iterators over data frame columns."""

from openclean.data.sequence import Sequence


def test_single_column_iterator(employees):
    """Test iterator for values in a single data frame column."""
    names = [n for n in Sequence(employees, 'Name')]
    assert len(names) == 7
    assert 'Alice' in names
    assert 'Bob' in names
    age = [a for a in Sequence(employees, 1)]
    assert len(age) == 7
    assert int(age[0]) == 23


def test_multi_column_iterator(employees):
    """Test iterator over tuples of values from multiple data frame columns."""
    salaries = [s for s in Sequence(employees, ['Name', 2])]
    assert len(salaries) == 7
    assert salaries[0] == ('Alice', 60000)
    assert salaries[1] == ('Bob', '')
