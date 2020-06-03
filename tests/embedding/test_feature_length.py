# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the string length feature function."""

from openclean.embedding.feature.length import NormalizedLength


def test_normalized_length_feature():
    """Test the normalized value length feature."""
    values = [1, 'AB', 'ABC', 1000]
    f = NormalizedLength().prepare(values)
    feature = [f.eval(v) for v in values]
    assert feature == [0, 1/3, 2/3, 1.0]
