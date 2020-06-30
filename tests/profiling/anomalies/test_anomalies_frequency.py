# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for frequency outlier detection operators."""

from openclean.function.value.normalize import MaxAbsScale
from openclean.profiling.anomalies.frequency import frequency_outliers
from openclean.profiling.util import geq


def test_absolute_frequency_outliers_for_single_column(agencies):
    """Test frequency outlier detection with absolute frequency counts."""
    # Value 'NY' occurs in 9 out of 10 rows in the dataset.
    outlier = frequency_outliers(agencies, 'state', geq(9), normalize=None)
    assert len(outlier) == 1
    assert outlier['NY'] == 9


def test_absolute_frequency_outliers_for_multiple_columns(agencies):
    """Test frequency outlier detection with absolute frequency counts for
    value pairs from multiple columns.
    """
    # The pair ('BK', 'NY') occurs in 6 rows and ('MN', 'NY') in two rows.
    outlier = frequency_outliers(
        agencies,
        ['borough', 'state'],
        geq(2),
        normalize=None
    )
    assert len(outlier) == 2
    assert outlier[('BK', 'NY')] == 6
    assert outlier[('MN', 'NY')] == 2


def test_normalized_frequency_outliers_for_single_column(agencies):
    """Test frequency outlier detection using normalized frequencies.
    """
    # By default, divide by total is used as the normalization function. The
    # state value 'NY' occurs in 9 out of 10 rows in the agencies dataset.
    outlier = frequency_outliers(agencies, 'state', geq(0.9))
    assert len(outlier) == 1
    assert outlier['NY'] == {'count': 9, 'frequency': 0.9}
    # Use MaxAbsScale as an alternative normalization function.
    outlier = frequency_outliers(
        agencies,
        'state',
        threshold=1,
        normalize=MaxAbsScale()
    )
    assert len(outlier) == 1
    assert outlier['NY'] == {'count': 9, 'frequency': 1.0}


def test_normalized_frequency_outliers_for_multi_columns(agencies):
    """Test frequency outlier detection using normalized frequencies for tuples
    of values from multiple columns.
    """
    # Use default divide by total as the normalization function. The pair
    # ('BK', 'NY') occurs in 6 out of ten rows in the agencies dataset.
    # state valye 'NY' occurs in 9 out of 10 rows in the agencies dataset.
    outlier = frequency_outliers(agencies, ['borough', 'state'], geq(0.9))
    assert len(outlier) == 0
    outlier = frequency_outliers(agencies, ['borough', 'state'], geq(0.6))
    assert len(outlier) == 1
    assert outlier[('BK', 'NY')] == {'count': 6, 'frequency': 0.6}
