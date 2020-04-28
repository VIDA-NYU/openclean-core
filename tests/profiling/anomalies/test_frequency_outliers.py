# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for frequency outlier detection operators."""

from openclean.profiling.anomalies import frequency_outliers
from openclean.function.predicate.scalar import Geq


def test_frequency_outlier(agencies):
    """Test frequency outlier detection operator."""
    # Single column outlier
    outlier = frequency_outliers(agencies, 'state', Geq(0.9))
    assert len(outlier) == 1
    assert outlier[0] == 'NY'
    # Multi-column outlier
    outlier = frequency_outliers(agencies, ['borough', 'state'], Geq(0.9))
    assert len(outlier) == 0
    outlier = frequency_outliers(agencies, ['borough', 'state'], Geq(0.6))
    assert len(outlier) == 1
    assert outlier[0] == ('BK', 'NY')
