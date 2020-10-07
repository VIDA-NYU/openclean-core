# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for GroupBy operator."""

from openclean.operator.map.groupby import groupby, GroupBy


# user defined functions - should consider cases with single and
# multiindices(tuples)
def use_first_last(b, s):
    return '_{}{}_{}{}'.format(b[0], b[-1], s[0], s[-1])


def test_groupby_operator_without_func(agencies):
    """ Test fd violation works correctly"""
    gpby = GroupBy(
        columns=['borough', 'state']
    ).map(agencies)
    assert gpby.get(('BK', 'NY')).shape[0] == 6
    assert gpby.get(('MN', 'NY')).shape[0] == 2
    assert ('BX', 'NY') in gpby.keys()


def test_groupby_operator_with_func(agencies):
    """ Test fd violation works correctly"""
    gpby = GroupBy(
        columns=['borough'],
        func=len
    ).map(agencies)
    assert len(gpby.keys()) == 1
    assert gpby.get(2).shape == (10, 3)

    gpby = GroupBy(
        columns=['borough', 'state'],
        func=use_first_last
    ).map(agencies)
    assert len(gpby.keys()) == 4
    assert gpby.get('_BK_NY').shape == (6, 3)
    assert {'_BK_NY', '_BX_NY', '_MN_NY', '_BX_NJ'} == gpby.keys()


def test_groupby_parking(parking):
    """Ensure that group-by works with data frames that have non-consecutive
    row identifier.
    """
    groups = groupby(parking, 'Meter Number')
    assert len(groups) > 0
    assert '144-3955' in groups
    assert groups.get('144-3955').shape == (8, 5)
