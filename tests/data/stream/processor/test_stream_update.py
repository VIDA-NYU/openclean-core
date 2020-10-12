# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the filter operator in data processing pipeline."""

from openclean.function.eval.base import Col


def test_update_rows_in_stream(stream311, boroughs):
    """Test updating city and borough values to lower case."""
    def all_lower(v1, v2):
        return str(v1).lower(), str(v2).lower()

    # convert all boroughs in the vocabulary to lower case.
    lower_boroughs = set({b.lower() for b in boroughs})
    # Convert all pairs with equal city and brough values to lower case.
    pairs = stream311\
        .select('city', 'borough')\
        .filter(Col('city') == Col('borough'))\
        .update('city', 'borough', all_lower)\
        .distinct()

    assert len(pairs) > 0
    for boro, _ in pairs:
        assert boro not in boroughs
        assert boro in lower_boroughs
