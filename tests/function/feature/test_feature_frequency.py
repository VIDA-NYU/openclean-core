# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the value frequency feature function."""

from openclean.function.feature.frequency import NormalizedFrequency


def test_normalized_frequency_feature():
    """Test the normalized value frequency feature."""
    values = [1, 2, 3, 4, 2, 3, 4, 3, 4, 4]
    f = NormalizedFrequency().get_function(data=values)
    feature = [f(v) for v in set(values)]
    assert feature == [0, 1/3, 2/3, 1.0]
