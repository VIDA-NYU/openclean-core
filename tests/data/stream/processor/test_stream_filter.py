# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the filter operator in data processing pipeline."""

from openclean.function.eval.aggregate import Max
from openclean.function.eval.base import Col


def test_sfilter_rows_from_stream(stream311, boroughs):
    """Test filtering rows where city and borough are identical."""
    ds = stream311\
        .select('city', 'borough')\
        .filter(Col('city') == Col('borough'))
    row_count = 0
    for rowid, row in ds.iterrows():
        assert len(row) == 2
        assert row[0] == row[1] in boroughs
        row_count += 1
    assert 0 < rowid <= 303
    assert 0 < row_count < 304
    # Add a row liit to the stream
    ds = stream311\
        .select('city', 'borough')\
        .where(Col('city') == Col('borough'), limit=10)
    row_count = 0
    for rowid, row in ds.iterrows():
        assert len(row) == 2
        assert row[0] == row[1] in boroughs
        row_count += 1
    assert row_count == 10


def test_filter_with_prepare(streamSchools):
    """Test filtering rows with a evaluation function that needs to be
    prepared.
    """
    ds = streamSchools\
        .select('borough', 'school_code', 'max_class_size')\
        .filter(Col('max_class_size') == Max('max_class_size'))\
        .to_df()
    print(ds)
