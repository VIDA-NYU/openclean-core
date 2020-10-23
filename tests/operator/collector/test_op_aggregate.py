# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for groupby aggregation operators."""

from openclean.operator.collector.aggregate import aggregate
from openclean.operator.map.groupby import groupby

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
