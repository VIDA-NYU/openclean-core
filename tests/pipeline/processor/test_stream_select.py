# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the select operator in data processing pipeline."""

import pandas as pd

from openclean.data.stream.df import DataFrameStream
from openclean.pipeline.processor.collector import DataFrameOperator
from openclean.pipeline.processor.producer import SelectOperator


def test_select_columns_from_stream():
    """Test selecting columns from a data stream."""
    df = pd.DataFrame(
        data=[[1, 2, 3], [3, 4, 5], [5, 6, 7]],
        columns=['A', 'B', 'C']
    )
    ds = DataFrameStream(df)
    df_sel = SelectOperator(columns=['A', 'C'])\
        .open(ds, ds.columns, downstream=[DataFrameOperator()])\
        .process(ds)
    assert df_sel.shape == (3, 2)
    assert list(df_sel.columns) == ['A', 'C']
    assert list(df_sel.iloc[0]) == [1, 3]
