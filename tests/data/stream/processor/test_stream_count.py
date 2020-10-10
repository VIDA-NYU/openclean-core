# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the count operator in data processing pipeline."""

from openclean.function.eval.base import Col


def test_count_rows_in_stream(stream311):
    """Test counting the number of rows in a stream."""
    # Total number of rows.
    rows = stream311.count()
    assert rows == 304
    rows = stream311.filter(Col('borough') == 'BROOKLYN').count()
    assert rows == 61


def test_count_values_in_stream(stream311):
    """Test counting the number of distrinct values in a column of the data
     stream.
     """
    # Total number of rows.
    rows = stream311.count('borough')
    assert rows == 5
    rows = stream311.filter(Col('borough') == 'BROOKLYN').count('borough')
    assert rows == 1
