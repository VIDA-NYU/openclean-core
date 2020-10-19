# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the data frame generator in data processing pipeline."""

import pandas as pd

from pandas.testing import assert_frame_equal


from openclean.data.stream.df import DataFrameStream
from openclean.pipeline.processor.collector import DataFrameOperator


def test_generate_df_from_stream():
    """Test creating a data frame from the rows in a stream."""
    data = [[1, 2, 3], [3, 4, 5], [5, 6, 7]]
    df = pd.DataFrame(data=data, columns=['A', 'B', 'C'])
    ds = DataFrameStream(df)
    consumer = DataFrameOperator().open(ds, ds.columns)
    assert_frame_equal(df, consumer.process(ds))
