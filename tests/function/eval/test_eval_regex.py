# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for regular expression match predicates for data frame rows."""

from openclean.function.eval.regex import IsMatch, IsNotMatch


def test_predicate_regex(employees):
    """Test is match and not is match predicates."""
    f = IsMatch(pattern='A', columns='Name').prepare(employees)
    assert f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])
    assert not f.eval(employees.iloc[2])
    # Full match
    f = IsMatch(pattern='A', fullmatch=True, columns='Name').prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert not f.eval(employees.iloc[1])


def test_predicate_nomatch(employees):
    """Test is not match predicate."""
    f = IsNotMatch(pattern='A', columns='Name').prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
    assert f.eval(employees.iloc[2])
    # Full match
    f = IsNotMatch(pattern='A', fullmatch=True, columns='Name')
    f = f.prepare(employees)
    assert f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
    f = IsNotMatch(pattern='A.+', fullmatch=True, columns='Name')
    f = f.prepare(employees)
    assert not f.eval(employees.iloc[0])
    assert f.eval(employees.iloc[1])
