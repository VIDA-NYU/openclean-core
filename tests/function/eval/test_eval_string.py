# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for string evaluation functions."""

import pandas as pd
import pytest

from openclean.function.eval.base import Col
from openclean.function.eval.string import (
    Capitalize, Concat, Format, Length, Lower, Upper, Split
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


def test_string_split_and_concat(people):
    """Test splitting and concatenating strings extracted from column values.
    """
    result = Split(Lower(Col('Name'))).eval(people)
    assert result == [['alice', 'davies'], ['bob', 'smith']]
    result = Concat(Split(Lower(Col('Name'))), '|').eval(people)
    assert result == ['alice|davies', 'bob|smith']


def test_string_upper(people):
    """Test string to upper characters function for column values."""
    names = Upper(Col('Name')).eval(people)
    assert names == ['ALICE DAVIES', 'BOB SMITH']
