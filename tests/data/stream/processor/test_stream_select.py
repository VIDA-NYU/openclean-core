# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the select operator in data processing pipeline."""


def test_select_columns_from_stream(stream311, boroughs):
    """Test selecting columns from a data stream."""
    select_2 = stream311.select('city', 'borough')
    for rowid, row in select_2.iterrows():
        assert len(row) == 2
        assert row[1] in boroughs
    assert rowid == 303
    select_1 = select_2.select('borough')
    for rowid, row in select_1.iterrows():
        assert len(row) == 1
        assert row[0] in boroughs
    assert rowid == 303
