# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the limit operator in data processing pipeline."""

from pandas.testing import assert_frame_equal

from openclean.profiling.column import DefaultColumnProfiler
from openclean.profiling.tests import ValueCounter


def test_dataset_profile_minmax(streamSchools):
    """Test geting (min,max) values for columns with multiple types from the
    dataset profile that wraps the profiler results.
    """
    # -- Test the default stream profiler -------------------------------------
    grades_1 = streamSchools.profile().minmax('grade')
    assert len(grades_1) == 2
    assert list(grades_1.loc['int']) == [1, 8]
    assert list(grades_1.loc['text']) == ['09-12', 'MS Core']
    # There is no difference for minmax between the two different profilers.
    grades_2 = streamSchools\
        .profile(default_profiler=DefaultColumnProfiler)\
        .minmax(2)
    assert_frame_equal(grades_1, grades_2)


def test_dataset_profile_stats(stream311):
    """Test getting data frame with basic column statistics using the dataset
    profile class that wraps the profiler results.
    """
    STATS_SCHEMA = ['total', 'empty', 'distinct', 'uniqueness', 'entropy']
    # -- Test the default stream profiler -------------------------------------
    df = stream311.profile().stats()
    assert df.shape == (4, len(STATS_SCHEMA))
    assert list(df.columns) == STATS_SCHEMA
    assert list(df.index) == stream311.columns
    # Column entropy is all None when using the default profiler.
    assert list(df.entropy) == [None] * df.shape[0]
    # -- Test the default column profiler that collects distinct values -------
    df = stream311.profile(default_profiler=DefaultColumnProfiler).stats()
    assert df.shape == (4, len(STATS_SCHEMA))
    assert list(df.columns) == STATS_SCHEMA
    assert list(df.index) == stream311.columns
    for e in df['entropy']:
        assert e >= 0


def test_dataset_profile_types(streamSchools):
    """Test getting data frame with data type statistics using the dataset
    profile class that wraps the profiler results.
    """
    TYPES_SCHEMA = ['float', 'int', 'text']
    # -- Test the default stream profiler -------------------------------------
    df = streamSchools.profile().types()
    assert df.shape == (6, len(TYPES_SCHEMA))
    assert list(df.columns) == TYPES_SCHEMA
    assert list(df.index) == streamSchools.columns
    # -- Test the default column profiler that collects distinct values -------
    df_dist = streamSchools\
        .profile(default_profiler=DefaultColumnProfiler)\
        .types(distinct=True)
    assert df_dist.shape == (6, len(TYPES_SCHEMA))
    assert list(df_dist.columns) == TYPES_SCHEMA
    assert list(df_dist.index) == streamSchools.columns
    # Ensure that the counts in the second data frame are all lower or equal
    # than the values in the first data frame.
    for i in df.index:
        for c in TYPES_SCHEMA:
            assert df_dist.at[i, c] <= df.at[i, c]
    # -- There should be one multi-type column with two types -----------------
    df = streamSchools.profile().multitype_columns().types()
    assert df.shape == (1, 2)


def test_dataset_profile_uniqueness(streamSchools):
    """Test getting unique columns using the dataset profile class that wraps
    the profiler results.
    """
    # -- Uniqueness can only be computed for the default column profiler ------
    profile = streamSchools.profile(default_profiler=DefaultColumnProfiler)
    # There are no unique columns in the dataset.
    df = profile.unique_columns().stats()
    assert df.shape == (0, 5)
    # School code is unique over the first 20 rows.
    profile = streamSchools\
        .limit(20)\
        .profile(default_profiler=DefaultColumnProfiler)
    df = profile.unique_columns().stats()
    assert df.shape == (1, 5)
    assert 'school_code' in df.index


def test_profile_all_columns(stream311):
    """Test profiling all columns in a given data stream."""
    metadata = stream311.profile()
    assert len(metadata) == 4


def test_profile_one_columns(stream311):
    """Test profiling a single column in a given data stream."""
    metadata = stream311.profile(profilers=0)
    assert len(metadata) == 1
    metadata = stream311.profile(profilers=(0, ValueCounter()))
    assert len(metadata) == 1


def test_profile_two_columns(stream311):
    """Test profiling all columns in a given data stream."""
    metadata = stream311.profile(profilers=[0, (2, ValueCounter())])
    assert len(metadata) == 2
