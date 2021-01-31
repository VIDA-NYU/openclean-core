# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for groupby aggregation operators."""

from openclean.operator.collector.aggregate import aggregate
from openclean.operator.map.groupby import groupby
import pandas as pd
import numpy as np

import pytest


def count_plates(group):
    # counts the unique plates in the group
    return len(group['Plate ID'])


def count_unique(S):
    # gets the length of the concatenated column from a Series
    return len(S.unique())


def test_groupby_agg(parking):
    """aggregate all groups in a DataFrameGrouping object using a callable that operates on each group.
    Here, count the number of plates with violations in each state
    """
    groups = groupby(parking, 'Registration State')
    agg = aggregate(groups=groups, schema=None, func=count_plates)

    assert agg.shape == (7, 1)
    assert agg.columns == ['count_plates']
    assert (agg['count_plates'] == [77, 2, 15, 3, 1, 1, 1]).all()


def test_agg_schema(parking):
    """ensure the schema is applied to the aggregations"""
    groups = groupby(parking, 'Registration State')
    agg = aggregate(groups=groups, schema=['Violations'], func=count_plates)

    assert agg.shape == (7, 1)
    assert agg.columns == ['Violations']
    assert (agg['Violations'] == [77, 2, 15, 3, 1, 1, 1]).all()

    with pytest.raises(TypeError):
        aggregate(groups=groups, schema=['Violations', 'Error'], func=count_plates)


def test_agg_multifunc(parking):
    """ensure a dictionary of column wise callables for aggregations can be passed to aggregate"""
    groups = groupby(parking, 'Registration State')
    agg = aggregate(groups=groups, schema=None, func={'Registration State': count_unique,
                                                      'Meter Number': count_unique,
                                                      'Plate ID': count_unique})

    assert agg.shape == (7, 3)
    assert (agg.columns == ['Registration State', 'Meter Number', 'Plate ID']).all()
    assert (agg['Registration State'] == [1, 1, 1, 1, 1, 1, 1]).all()
    assert (agg['Meter Number'] == [53, 2, 12, 3, 1, 1, 1]).all()
    assert (agg['Plate ID'] == [74, 2, 15, 3, 1, 1, 1]).all()

    with pytest.raises(KeyError):
        aggregate(groups=groups, schema=None, func={'Registration State': count_unique,
                                                    'Plate XYZ': len})


'''
    test cases for different types of possible aggregate functions
'''
STUDENTS = pd.DataFrame({'roll#': [1, 3, 51, 1, 4, 3, 3],
                         'zipcode': [1223, 23, 'poi', np.nan, '23', 2, '23'],
                         'name': ['sam', 'ben', 'pat', 'hugh', 'jake', 'jen', 'ben']})


# case 1: input df, output single value - (SISO) single-in-single-out
def get_size(group):
    return str(group.shape)


def test_agg_siso():
    groups = groupby(STUDENTS, 'roll#')

    # case 1
    agg = aggregate(groups=groups, func=get_size)
    assert agg.shape == (4, 1)
    assert agg.columns == ['get_size']
    assert (agg['get_size'] == ['(2, 3)', '(3, 3)', '(1, 3)', '(1, 3)']).all()

    # case 1 w schema
    agg = aggregate(groups=groups, func=get_size, schema=['size'])
    assert agg.shape == (4, 1)
    assert agg.columns == ['size']
    assert (agg['size'] == ['(2, 3)', '(3, 3)', '(1, 3)', '(1, 3)']).all()


# case 2: input df, output dict - (SIMO) single-in-mult-out
def get_most_frequent(group):
    from collections import Counter
    row = dict()
    for cols in group.columns:
        # incase multiple values with same count, returns top alphabetically/numerically
        # remember: (nan = last alphabetically, but first numerically)
        row[cols] = Counter(group[cols]).most_common(1)[0][0]
    return row


def test_agg_simo():
    groups = groupby(STUDENTS, 'roll#')

    # case 2
    agg = aggregate(groups=groups, func=get_most_frequent)
    assert agg.shape == (4, 3)
    assert (agg.columns == ['roll#', 'zipcode', 'name']).all()
    assert agg.loc[3]['roll#'] == 3
    assert agg.loc[3]['name'] == 'ben'
    assert agg.loc[3]['zipcode'] == 23

    # case 2 w schema
    agg = aggregate(groups=groups, func=get_most_frequent, schema=['roll', 'zip', 'name'])
    assert (agg.columns == ['roll', 'zip', 'name']).all()


# case 3: input series, output single - (MISO) mult-in-single-out
# (multiple series can go into this func in the same aggregation and each returns a single value)
def test_agg_miso():
    groups = groupby(STUDENTS, 'roll#')

    func = {
        'name': len,  # length of the series
        'zipcode': get_size
    }

    # case 3
    agg = aggregate(groups=groups, func=func)
    assert agg.shape == (4, 2)
    assert (agg.columns == ['name', 'zipcode']).all()
    assert (agg.name == [2, 3, 1, 1]).all()

    # case 3 raises
    with pytest.raises(KeyError):
        func = {
            'a': len,  # incorrect column names provided
            'b': get_size
        }
        aggregate(groups=groups, func=func)


# case 4: input series, output series/dict - (MIMO) mult-in-mult-out
# (multiple series can go into this func in the same aggregation and each returns a dict or a series)
def get_profile_series_dict(S):
    return {
        'sum': S.sum(),
        'len': len(S.sum())
    }


def get_profile_series(S):
    return pd.Series(get_profile_series_dict(S))


def test_agg_mimo():
    groups = groupby(STUDENTS, 'roll#')

    func = {
        'zipcode': len,  # returns single value
        'name': get_profile_series_dict  # returns dict
    }

    # case 4
    agg = aggregate(groups=groups, func=func)
    assert agg.shape == (4, 2)
    assert (agg.columns == ['zipcode', 'name']).all()
    assert (agg['zipcode'] == [2, 3, 1, 1]).all()
    for val in agg['name'].values:
        assert isinstance(val, dict)
        assert 'sum' in val
        assert 'len' in val
        assert len(val) == 2
