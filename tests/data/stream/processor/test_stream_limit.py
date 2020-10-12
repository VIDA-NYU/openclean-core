# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the limit operator in data processing pipeline."""

import pytest


@pytest.mark.parametrize('limit', [0, 10, 100])
def test_stream_limit(limit, stream311, boroughs):
    """Test getting a distinct set of borough names for the first n rows."""
    # Distinct with column select.
    dist_boroughs = stream311.limit(limit).distinct('borough')
    for b in dist_boroughs:
        assert 0 < dist_boroughs[b] <= limit
        assert b in boroughs
    assert sum(dist_boroughs.values()) == limit


def test_stream_full_set(stream311):
    """Ensure that all rows are returned when the limit is greater than the
    number of rows in the dataset.
    """
    dist_boroughs = stream311.limit(1000).distinct('borough')
    assert sum(dist_boroughs.values()) == 304
