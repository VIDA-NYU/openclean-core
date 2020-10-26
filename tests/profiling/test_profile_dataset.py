# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the dataset profiling consumer."""

import pandas as pd

from openclean.profiling.column import DefaultStreamProfiler
from openclean.profiling.dataset import ProfileConsumer, dataset_profile
from openclean.profiling.tests import ValueCounter


# -- Consumer -----------------------------------------------------------------

def test_profile_consumer():
    """Test profiling two columns over a short data stream."""
    consumer = ProfileConsumer(
        profilers=[(0, 'A', ValueCounter()), (2, 'B', ValueCounter())]
    )
    consumer.consume(0, [1, 2, 3])
    consumer.consume(1, [1, 5, 6])
    results = consumer.close()
    assert len(results) == 2
    assert results[0] == {'column': 'A', 'stats': {1: 2}}
    assert results[1] == {'column': 'B', 'stats': {3: 1, 6: 1}}


# -- Column filter ------------------------------------------------------------


def test_profile_all_columns(nyc311):
    """Test profiling all columns in a given data frame."""
    profile = dataset_profile(nyc311)
    assert len(profile) == 4
    assert profile.columns == list(nyc311.columns)


def test_profile_one_column(nyc311):
    """Test profiling a single column in a data frame."""
    profile = dataset_profile(nyc311, profilers=[0])
    assert len(profile) == 1
    assert profile.columns == ['descriptor']
    # By default the column profiler is used that collects distinct values.
    assert 'distinctValueCount' in profile[0]['stats']
    profile = dataset_profile(nyc311, profilers=[(0, DefaultStreamProfiler())])
    assert len(profile) == 1
    assert profile.columns == ['descriptor']
    # The stream profiler does not count distinct values.
    assert 'distinctValueCount' not in profile[0]['stats']


def test_profile_two_columns(nyc311):
    """Test profiling a two columns in a data frame."""
    profile = dataset_profile(
        nyc311,
        profilers=[0, (2, DefaultStreamProfiler())]
    )
    assert len(profile) == 2
    assert profile.columns == ['descriptor', 'city']


# -- Dataset Profile ----------------------------------------------------------

def test_dataset_profile_minmax(schools):
    """Test geting (min,max) values for columns with multiple types from the
    dataset profile that wraps the profiler results.
    """
    # -- Test the default stream profiler -------------------------------------
    grades_1 = dataset_profile(schools).minmax('grade')
    assert len(grades_1) == 3
    assert list(grades_1.loc['int']) == [1, 8]
    assert list(grades_1.loc['str']) == ['09-12', 'MS Core']


def test_dataset_profile_stats(nyc311):
    """Test getting data frame with basic column statistics using the dataset
    profile class that wraps the profiler results.
    """
    STATS_SCHEMA = ['total', 'empty', 'distinct', 'uniqueness', 'entropy']
    # -- Test the default stream profiler -------------------------------------
    df = dataset_profile(nyc311).stats()
    assert df.shape == (4, len(STATS_SCHEMA))
    assert list(df.columns) == STATS_SCHEMA
    assert list(df.index) == list(nyc311.columns)
    for e in df['entropy']:
        assert e >= 0
    # -- Test the default column profiler that collects distinct values -------
    df = dataset_profile(nyc311, default_profiler=DefaultStreamProfiler)\
        .stats()
    assert df.shape == (4, len(STATS_SCHEMA))
    assert list(df.columns) == STATS_SCHEMA
    assert list(df.index) == list(nyc311.columns)
    # Column entropy is all None when using the default profiler.
    assert list(df.entropy) == [None] * df.shape[0]


def test_dataset_profile_types(schools):
    """Test getting data frame with data type statistics using the dataset
    profile class that wraps the profiler results.
    """
    TYPES_SCHEMA = ['float', 'int', 'str']
    # -- Test the default stream profiler -------------------------------------
    df_dist = dataset_profile(schools).types()
    assert df_dist.shape == (6, len(TYPES_SCHEMA))
    assert list(df_dist.columns) == TYPES_SCHEMA
    assert list(df_dist.index) == list(schools.columns)
    # -- Test the default column profiler that collects distinct values -------
    df = dataset_profile(schools, default_profiler=DefaultStreamProfiler)\
        .types()
    assert df.shape == (6, len(TYPES_SCHEMA))
    assert list(df.columns) == TYPES_SCHEMA
    assert list(df.index) == list(schools.columns)
    # Ensure that the counts in the second data frame are all lower or equal
    # than the values in the first data frame.
    for i in df.index:
        for c in TYPES_SCHEMA:
            assert df_dist.at[i, c] <= df.at[i, c]
    # -- There should be one multi-type column with two types -----------------
    df = dataset_profile(schools).multitype_columns().types()
    assert df.shape == (1, 3)


def test_dataset_profile_uniqueness():
    """Test getting unique columns using the dataset profile class that wraps
    the profiler results.
    """
    data = [[1, 2, 3], [2, 2, 3], [3, 4, 5]]
    df = pd.DataFrame(data=data, columns=['A', 'B', 'C'])
    # -- Uniqueness can only be computed for the default column profiler ------
    profile = dataset_profile(df)
    # There are no unique columns in the dataset.
    df = profile.unique_columns().stats()
    assert df.shape == (1, 5)
    assert 'A' in df.index
