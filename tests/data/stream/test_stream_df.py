# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for streaming pandas data frames."""

from openclean.data.stream.df import DataFrameStream


def test_stream_data_frame(nyc311):
    """Test streaming a pandas data frame."""
    ds = DataFrameStream(nyc311)
    assert ds.columns == ['descriptor', 'borough', 'city', 'street']
    for rowid, row in ds.iterrows():
        assert len(row) == 4
    assert rowid == 303
