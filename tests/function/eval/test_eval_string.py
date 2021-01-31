# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for string evaluation functions."""

import pandas as pd
import pytest

from openclean.function.eval.base import Col
from openclean.function.eval.text import (
    Capitalize, Concat, EndsWith, Format, Length, Lower, StartsWith, Upper
)


@pytest.fixture
def people():
    return pd.DataFrame(
        data=[
            ['alice davies', 23],
            ['bob Smith', 33]
        ],
        columns=['Name', 'Age']
    )


def test_string_capitalize(people):
    """Test string capitalization function for column values."""
    op = Capitalize(Col('Name'))
    names = op.eval(people)
    assert names == ['Alice davies', 'Bob smith']
    f = op.prepare(people.columns)
    names = [f(row) for row in people.itertuples(index=False, name=None)]
    assert names == ['Alice davies', 'Bob smith']


def test_string_concat(people):
    """Test string concatenation function for column values."""
    op = Concat(columns=['Name', 'Age'], delimiter=' ', as_string=True)
    names = op.eval(people)
    assert names == ['alice davies 23', 'bob Smith 33']


def test_string_endswith(people):
    """Test string ends with predicate."""
    matches = EndsWith(Col('Name'), 'Smith').eval(people)
    assert matches == [False, True]


def test_string_format(people):
    """Test formating strings using one or more values extracted from data
    frame columns.
    """
    tmpl = 'My name is {}'
    op = Format(tmpl, 'Name')
    texts = op.eval(people)
    results = [tmpl.format('alice davies'), tmpl.format('bob Smith')]
    assert texts == results
    f = op.prepare(people.columns)
    texts = [f(row) for row in people.itertuples(index=False, name=None)]
    assert texts == results


def test_string_length(people):
    """Test string character count function for column values."""
    op = Length(Col('Name'))
    length = op.eval(people)
    assert length == [12, 9]
    f = op.prepare(people.columns)
    length = [f(row) for row in people.itertuples(index=False, name=None)]
    assert length == [12, 9]


def test_string_lower(people):
    """Test string to lower characters function for column values."""
    names = Lower(Col('Name')).eval(people)
    assert names == ['alice davies', 'bob smith']


def test_string_startswith(people):
    """Test string starts with predicate."""
    matches = StartsWith(Col('Name'), 'alice').eval(people)
    assert matches == [True, False]


def test_string_upper(people):
    """Test string to upper characters function for column values."""
    names = Upper(Col('Name')).eval(people)
    assert names == ['ALICE DAVIES', 'BOB SMITH']
