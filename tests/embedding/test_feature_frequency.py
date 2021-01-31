# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the value frequency feature function."""

from openclean.embedding.feature.frequency import NormalizedFrequency


def test_normalized_frequency_feature():
    """Test the normalized value frequency feature."""
    values = [1, 2, 3, 4, 2, 3, 4, 3, 4, 4]
    f = NormalizedFrequency().prepare(values)
    features = [f.eval(v) for v in [1, 2, 3, 4]]
    assert features == [0, 1/3, 2/3, 1.0]
