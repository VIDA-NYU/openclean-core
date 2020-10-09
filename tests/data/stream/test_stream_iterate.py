# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for CSV file streams."""

import os

from openclean.data.load import stream


"""Input files for testing."""
DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../.files')
NYC311_FILE = os.path.join(DIR, '311-descriptor.csv')


def test_iterate_over_raw_input():
    """Test iterating over all rows in a input file without any consumers for
    data processing.
    """
    ds = stream(NYC311_FILE)
    for rowid, row in ds.iterrows():
        assert len(row) == 4
    assert rowid == 303


def test_iterate_with_select():
    """Test iterating over all rows in a input file without any consumers for
    data processing.
    """
    df = stream(NYC311_FILE)\
        .select('street', 'city', 'borough')\
        .select('borough')\
        .to_df()
    print(df)
