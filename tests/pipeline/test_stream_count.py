# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the count operator in data processing pipelines."""


def test_count_rows_in_stream(ds):
    """Test counting the number of rows in a stream."""
    # The number of rows in the fixed data stream is 10.
    assert ds.count() == 10
