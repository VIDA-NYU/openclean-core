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
    f = Capitalize(Col('Name')).prepare(people)
    names = []
    for _, values in people.iterrows():
        names.append(f.eval(values))
    assert names == ['Alice davies', 'Bob smith']
    # Test for non-string columns.
    f = Capitalize(Col('Age')).prepare(people)
    with pytest.raises(ValueError):
        f.eval(people.iloc[0])
    f = Capitalize(Col('Age'), as_string=True).prepare(people)
    assert f.eval(people.iloc[0]) == '23'


def test_string_format(people):
    """Test formating strings using one or more values extracted from data
    frame columns.
    """
    f = Format(Col('Name'), 'My name is {}').prepare(people)
    assert f.eval(people.iloc[0]) == 'My name is alice davies'
    f = Format(Capitalize(Split(Col('Name'))), '{1}, {0}').prepare(people)
    assert f.eval(people.iloc[0]) == 'Davies, Alice'
    f = Format(Col(['Name', 'Age']), '{} is {}').prepare(people)
    assert f.eval(people.iloc[0]) == 'alice davies is 23'
    # Use a list of producers.
    f = Format([Col('Age'), Col('Name')], '{1} is {0}').prepare(people)
    assert f.eval(people.iloc[0]) == 'alice davies is 23'


def test_string_length(people):
    """Test string character count function for column values."""
    f = Length(Col('Name')).prepare(people)
    length = []
    for _, values in people.iterrows():
        length.append(f.eval(values))
    assert length == [12, 9]


def test_string_lower(people):
    """Test string to lower characters function for column values."""
    f = Lower(Col('Name')).prepare(people)
    names = []
    for _, values in people.iterrows():
        names.append(f.eval(values))
    assert names == ['alice davies', 'bob smith']


def test_string_split_and_concat(people):
    """Test splitting and concatenating strings extracted from column values.
    """
    f = Split(Lower(Col('Name'))).prepare(people)
    assert f.eval(people.iloc[1]) == ['bob', 'smith']
    f = Concat(Split(Lower(Col('Name'))), '|').prepare(people)
    assert f.eval(people.iloc[1]) == 'bob|smith'


def test_string_upper(people):
    """Test string to upper characters function for column values."""
    f = Upper(Col('Name')).prepare(people)
    names = []
    for _, values in people.iterrows():
        names.append(f.eval(values))
    assert names == ['ALICE DAVIES', 'BOB SMITH']
