# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the data frame generator in data processing pipelines."""

import pandas as pd

from pandas.testing import assert_frame_equal


from openclean.pipeline import stream


def test_generate_df_from_stream():
    """Test creating a data frame from the rows in a stream."""
    data = [[1, 2, 3], [3, 4, 5], [5, 6, 7]]
    df = pd.DataFrame(data=data, columns=['A', 'B', 'C'], dtype=object) # switching off pandas type detection
    ds = stream(df).to_df()
    assert_frame_equal(df, ds)
