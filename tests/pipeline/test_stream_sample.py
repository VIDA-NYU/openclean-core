# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the sample operator in data processing pipelines."""

import pytest


@pytest.mark.parametrize('size', [0, 3, 5, 7, 10, 20])
def test_sample_rows_in_stream(size, ds):
    """Test counting the number of rows in a stream sample."""
    assert ds.sample(size=size).count() == min(size, 10)
