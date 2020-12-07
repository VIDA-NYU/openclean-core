# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for GroupBy operator."""

from openclean.function.eval.base import Eval
from openclean.operator.map.groupby import groupby, GroupBy


# User-defined funtion that returns the first character of a given string.
def get_first(val):
    return val[0]


# User-defined functions - should consider cases with single and
# multiindices(tuples)
def use_first_last(b, s):
    return '_{}{}_{}{}'.format(b[0], b[-1], s[0], s[-1])


def test_groupby_parking(parking):
    """Ensure that group-by works with data frames that have non-consecutive
    row identifier.
    """
    groups = groupby(parking, 'Meter Number')
    assert len(groups) > 0
    assert '144-3955' in groups
    assert groups.get('144-3955').shape == (8, 5)


def test_groupby_selection(parking):
    """Ensure groupby is able to select groups with n=3 elements."""
    groups = groupby(parking, 'Street', having=3)
    assert len(groups) > 0
    assert '2nd Ave' in groups
    assert groups.get('2nd Ave').shape == (3, 5)


def test_groupby_with_eval(schools):
    """Test groupby operator with key values that are generated using evaluation
    functions.
    """
    # Group by first character of school code.
    first_char = Eval(columns='school_code', func=get_first)
    groups = GroupBy(columns=first_char).map(schools)
    assert groups.keys() == {'K', 'M', 'Q', 'R', 'X'}
    # Group by using columns that are a combination of column name and an
    # eval function. Apply additional concatenation function on the extracted
    # values to generate the group by key.
    groups = GroupBy(
        columns=['borough', first_char],
        func=lambda x, y: '{}_{}'.format(x, y)
    ).map(schools)
    assert groups.keys() == {'K_K', 'M_M', 'Q_Q', 'R_R', 'X_X'}


def test_groupby_without_func(agencies):
    """Thes group_by operator on keys that are extracted directly from a
    pair of columns.
    """
    groups = GroupBy(columns=['borough', 'state']).map(agencies)
    assert groups.get(('BK', 'NY')).shape[0] == 6
    assert groups.get(('MN', 'NY')).shape[0] == 2
    assert ('BX', 'NY') in groups.keys()


def test_groupby_with_func(agencies):
    """Test group_by operator that applies a given function on key values that
    are extracted from data frame columns.
    """
    groups = GroupBy(columns=['borough'], func=len).map(agencies)
    assert len(groups.keys()) == 1
    assert groups.get(2).shape == (10, 3)

    groups = GroupBy(columns=['borough', 'state'], func=use_first_last).map(agencies)
    assert len(groups.keys()) == 4
    assert groups.get('_BK_NY').shape == (6, 3)
    assert {'_BK_NY', '_BX_NY', '_MN_NY', '_BX_NJ'} == groups.keys()
