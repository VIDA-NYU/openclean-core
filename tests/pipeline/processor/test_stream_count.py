# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the count operator in data processing pipeline."""

from openclean.data.stream.df import DataFrameStream
from openclean.pipeline.consumer.collector import RowCount
from openclean.pipeline.processor.collector import CollectOperator


def test_count_rows_in_stream(employees):
    """Test counting the number of rows in a stream."""
    # Use a stream over the emlployees data frame. The data frame contains
    # seven rows.
    ds = DataFrameStream(employees)
    assert CollectOperator(RowCount).open(ds, ds.columns).process(ds) == 7
