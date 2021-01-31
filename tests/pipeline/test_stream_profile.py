# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the profile operator in data processing pipelines."""


def test_profile_data_stream(ds):
    """Test profiling all columns in a data stream."""
    # Assert that converting all values in column 'B' to string without
    # type cast yields values of type 'str'.
    profile = ds.profile()
    assert len(profile) == 3
    types = profile.types()
    assert list(types.columns) == ['int', 'str']
    assert list(types.index) == ['A', 'B', 'C']
