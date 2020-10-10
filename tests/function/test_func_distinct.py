# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the distinct value count generator."""

from openclean.function.value.normalize import (
    divide_by_total, MaxAbsScale, MinMaxScale
)
from openclean.operator.collector.distinct import distinct


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


def test_distinct_normalized(schools):
    """Test frequency normalization for distinct value counts."""
    # Count frequencies of the different boroughs
    boroughs = distinct(
        schools,
        columns='borough',
        normalizer=divide_by_total,
        keep_original=True,
        labels=['count', 'normalized_count']
    )
    assert len(boroughs) == 5
    assert boroughs['K']['normalized_count'] == boroughs['K']['count'] / 100
    assert boroughs['M']['normalized_count'] == boroughs['M']['count'] / 100
    assert boroughs['Q']['normalized_count'] == boroughs['Q']['count'] / 100
    assert boroughs['R']['normalized_count'] == boroughs['R']['count'] / 100
    assert boroughs['X']['normalized_count'] == boroughs['X']['count'] / 100
    boroughs = distinct(schools, columns='borough', normalizer=MaxAbsScale)
    assert len(boroughs) == 5
    assert boroughs['K'] == 36 / 36
    assert boroughs['M'] == 16 / 36
    assert boroughs['Q'] == 17 / 36
    assert boroughs['R'] == 5 / 36
    assert boroughs['X'] == 26 / 36
    boroughs = distinct(schools, columns='borough', normalizer=MinMaxScale())
    assert len(boroughs) == 5
    assert boroughs['K'] == 31 / 31
    assert boroughs['M'] == 11 / 31
    assert boroughs['Q'] == 12 / 31
    assert boroughs['R'] == 0 / 31
    assert boroughs['X'] == 21 / 31


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
