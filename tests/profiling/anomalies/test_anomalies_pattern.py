# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit test for regular expression outlier detection operators."""

from openclean.profiling.anomalies.pattern import regex_outliers


def test_regex_outliers_for_data_frame(nyc311):
    """Test regular expression outlier detection operator for a column in a
    data frame.
    """
    # Empty pattern set results in no outliers.
    outlier = regex_outliers(df=nyc311, columns='city', patterns=[])
    assert len(outlier) == 0
    # Single pattern for city names that do no start with B, M, Q or S.
    outlier = regex_outliers(df=nyc311, columns='city', patterns=[r'[BMQS].+'])
    assert len(outlier) == 24
    for boro in ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']:
        assert boro not in outlier
    # Multi-patterns for the four borough start characters
    patterns = [r'{}.+'.format(c) for c in 'BMQS']
    outlier = regex_outliers(df=nyc311, columns='city', patterns=patterns)
    assert len(outlier) == 24
    for boro in ['BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND']:
        assert boro not in outlier
