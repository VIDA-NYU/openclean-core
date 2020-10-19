# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the filter operator in data processing pipeline."""

import pandas as pd

from openclean.data.stream.df import DataFrameStream
from openclean.function.eval.base import Col
from openclean.pipeline.processor.collector import DataFrameOperator
from openclean.pipeline.processor.producer import UpdateOperator


def test_update_rows_in_stream():
    """Test updating values in a column in a data stream."""
    df = pd.DataFrame(
        data=[[1, 2, 3], [3, 4, 5], [5, 6, 7]],
        columns=['A', 'B', 'C']
    )
    ds = DataFrameStream(df)
    df_upd = UpdateOperator(columns=['A'], func=Col('A') + Col('C'))\
        .open(ds, ds.columns, downstream=[DataFrameOperator()])\
        .process(ds)
    assert list(df_upd['A']) == [4, 8, 12]
