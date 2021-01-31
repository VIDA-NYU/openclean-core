# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the typecase operator in data processing pipelines."""

from openclean.function.eval.datatype import Str


def test_typcast_for_data_stream(ds):
    """Test typecast for column values in a data stream."""
    # Assert that converting all values in column 'B' to string without
    # type cast yields values of type 'str'.
    values = ds.update('A', Str('B')).distinct('A').keys()
    for v in values:
        assert isinstance(v, str)
    # Convert values in column 'B' to string and then typecast.
    values = ds.update('A', Str('B')).typecast().distinct('A').keys()
    for v in values:
        assert isinstance(v, int)
