# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for frequency outlier detection operators."""

from openclean.function.value.normalize import MaxAbsScale
from openclean.profiling.anomalies.frequency import frequency_outliers
from openclean.util.threshold import ge


def test_absolute_frequency_outliers_for_single_column(agencies):
    """Test frequency outlier detection with absolute frequency counts."""
    # Value 'NY' occurs in 9 out of 10 rows in the dataset.
    outlier = frequency_outliers(agencies, 'state', ge(9), normalize=None)
    assert len(outlier.values()) == 1
    assert outlier[0] == {'value': 'NY', 'metadata': {'count':  9}}


def test_absolute_frequency_outliers_for_multiple_columns(agencies):
    """Test frequency outlier detection with absolute frequency counts for
    value pairs from multiple columns.
    """
    # The pair ('BK', 'NY') occurs in 6 rows and ('MN', 'NY') in two rows.
    outliers = frequency_outliers(
        agencies,
        ['borough', 'state'],
        ge(2),
        normalize=None
    )
    assert len(outliers.values()) == 2
    counts = outliers.counts()
    assert counts[('BK', 'NY')] == 6
    assert counts[('MN', 'NY')] == 2


def test_normalized_frequency_outliers_for_single_column(agencies):
    """Test frequency outlier detection using normalized frequencies.
    """
    # By default, divide by total is used as the normalization function. The
    # state value 'NY' occurs in 9 out of 10 rows in the agencies dataset.
    outliers = frequency_outliers(agencies, 'state', ge(0.9))
    assert len(outliers.values()) == 1
    assert outliers.counts()['NY'] == 9
    assert outliers.frequencies()['NY'] == 0.9
    # Use MaxAbsScale as an alternative normalization function.
    outliers = frequency_outliers(
        agencies,
        'state',
        threshold=1,
        normalize=MaxAbsScale()
    )
    assert len(outliers) == 1
    assert outliers.frequencies()['NY'] == 1.0


def test_normalized_frequency_outliers_for_multi_columns(agencies):
    """Test frequency outlier detection using normalized frequencies for tuples
    of values from multiple columns.
    """
    # Use default divide by total as the normalization function. The pair
    # ('BK', 'NY') occurs in 6 out of ten rows in the agencies dataset.
    # state valye 'NY' occurs in 9 out of 10 rows in the agencies dataset.
    outlier = frequency_outliers(agencies, ['borough', 'state'], ge(0.9))
    assert len(outlier.values()) == 0
    outlier = frequency_outliers(agencies, ['borough', 'state'], ge(0.6))
    assert len(outlier.values()) == 1
    assert outlier[0]['metadata'] == {'count': 6, 'frequency': 0.6}
