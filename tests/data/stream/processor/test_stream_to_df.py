# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the data frame generator in data processing pipeline."""


def test_generate_df_from_stream(stream311):
    """Test creating a data frame from the rows in a stream."""
    # Data frame for the full stream.
    df = stream311.to_df()
    assert df.shape == (304, 4)
    assert list(df.columns) == ['descriptor', 'borough', 'city', 'street']
    # Data frame for only two columns of the stream.
    df = stream311.select('city', 'borough').to_df()
    assert df.shape == (304, 2)
    assert list(df.columns) == ['city', 'borough']
