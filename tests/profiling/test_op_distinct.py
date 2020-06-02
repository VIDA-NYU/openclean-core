# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the distinct value set profiling operator."""

from openclean.profiling.feature.distinct import distinct, distinct_values


def test_distinct_all_columns(schools):
    """Test getting distinct values over all columns."""
    tuples = distinct(schools)
    assert len(tuples) == schools.shape[0]
    for key in tuples.keys():
        assert tuples[key] == 1


def test_distinct_multi_columns(schools):
    """Test getting distinct values for a multiple columns."""
    boro_grade = distinct(schools, columns=['borough', 'grade'])
    assert len(boro_grade) == 37
    assert boro_grade[('K', '09-12')] == 14
    assert sum(v for v in boro_grade.values()) == 100


def test_distinct_single_column(schools):
    """Test getting distinct values for a single column."""
    # Count frequencies of the different boroughs
    boroughs = distinct(schools, columns='borough')
    # The result is a dictionary with five elements.
    assert len(boroughs) == 5
    assert boroughs['K'] == 36
    assert boroughs['M'] == 16
    assert boroughs['Q'] == 17
    assert boroughs['R'] == 5
    assert boroughs['X'] == 26
    # Get only the list of distinct values
    boroughs = sorted(distinct_values(schools, columns='borough'))
    assert boroughs == ['K', 'M', 'Q', 'R', 'X']
