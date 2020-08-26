# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for GroupBy operator."""

from openclean.operator.map.groupby import GroupBy

# user defined functions - should consider cases with single and multiindices(tuples)
def use_first_last(index):
    if type(index) == tuple:  # multiindex
        grouper = str()
        for t in index:
            grouper += '_' + t[0] + t[-1]
        return grouper

    return index[0] + index[-1]

def use_size(index):
    if type(index) == tuple:  # multiindex
        grouper = 0
        for t in index:
            grouper += len(t)
        return grouper

    return len(index)

def test_groupby_operator_without_func(agencies):
    """ Test fd violation works correctly"""
    gpby = GroupBy(
        on=['borough','state']
    ).map(agencies)
    assert gpby.get(('BK', 'NY')).shape[0] == 6
    assert gpby.get(('MN', 'NY')).shape[0] == 2
    assert ('BX', 'NY') not in gpby.keys()


def test_groupby_operator_with_func(agencies):
    """ Test fd violation works correctly"""
    gpby = GroupBy(
        on=['borough','state'],
        func=use_size
    ).map(agencies)
    assert len(gpby.keys()) == 1
    assert gpby.get(4).shape[0] == (10, 3)

    gpby = GroupBy(
        on=['borough', 'state'],
        func=use_first_last
    ).map(agencies)
    assert len(gpby.keys()) == 4
    assert gpby.get('_BK_NY').shape == (6, 3)
    assert {'_BK_NY', '_BX_NY', '_MN_NY', '_BX_NJ'} == gpby.keys()
