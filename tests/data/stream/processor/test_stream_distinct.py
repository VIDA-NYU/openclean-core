# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the distinct operator in data processing pipeline."""


def test_distinct_boroughs_from_stream(stream311, boroughs):
    """Test getting a distinct set of borough names."""
    # Distinct with column select.
    dist_boroughs = stream311.distinct('borough')
    assert set(dist_boroughs.keys()) == boroughs
    for b in boroughs:
        assert dist_boroughs[b] > 0
    # Distinct with explicit select.
    dist_boroughs = stream311.select('borough').distinct()
    assert set(dist_boroughs.keys()) == boroughs
    for b in boroughs:
        assert dist_boroughs[b] > 0
