# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the distinct operator in data processing pipelines."""

from collections import Counter


def test_count_distinct_rows(ds):
    """Test distinct count over a stream of rows."""
    assert len(ds.distinct()) == 10
    count_a = ds.distinct('A')
    assert count_a == Counter({'A': 10})
