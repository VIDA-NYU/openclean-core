# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the limit operator in data processing pipeline."""

import pandas as pd

from openclean.data.stream.df import DataFrameStream
from openclean.pipeline.consumer.collector import RowCount
from openclean.pipeline.processor.collector import CollectOperator
from openclean.pipeline.processor.producer import LimitOperator


def test_stream_limit():
    """Test limiting the number of rows that are passed on to a downstream
    consumer.
    """
    # Create a data frame with three rows.
    ds = DataFrameStream(pd.DataFrame(data=[[1], [2], [3]], columns=['A']))
    # For limits that are lower or equal the row count everything should
    # work fine.
    for i in range(4):
        consumer = LimitOperator(limit=i)\
            .open(ds, ds.columns, downstream=[CollectOperator(RowCount)])
        try:
            consumer.process(ds)
        except StopIteration:
            pass
        assert consumer.close() == i
    # If the limit is greater than the number of rows no exception is raised.
    rows = LimitOperator(limit=10)\
        .open(ds, ds.columns, downstream=[CollectOperator(RowCount)])\
        .process(ds)
    assert rows == 3
