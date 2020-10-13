# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for iterrows function of an empty data processing pipeline."""


def test_iterate_over_raw_input(stream311):
    """Test iterating over all rows in a input file without any consumers for
    data processing.
    """
    for rowid, row in stream311.iterrows():
        assert len(row) == 4
    assert rowid == 303
