# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the distinct operator in data processing pipeline."""

import pandas as pd

from openclean.data.stream.df import DataFrameStream
from openclean.pipeline.consumer.collector import Distinct
from openclean.pipeline.processor.collector import CollectOperator


def test_count_distinct_rows():
    """Test distinct count over a stream of rows."""
    # Create a data frame with five rows (two distinct rows)
    ROW_1 = [1, 2, 3]
    ROW_2 = [3, 4, 5]
    rows = [ROW_1, ROW_2, ROW_1, ROW_2, ROW_1]
    df = pd.DataFrame(data=rows, columns=['A', 'B', 'C'])
    ds = DataFrameStream(df)
    counts = CollectOperator(Distinct).open(ds, ds.columns).process(ds)
    assert len(counts) == 2
    assert counts[(1, 2, 3)] == 3
    assert counts[(3, 4, 5)] == 2
