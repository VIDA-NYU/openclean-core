# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the move operator in data processing pipelines."""


def test_move_columns_in_stream(ds):
    """Test moving columns in the data stream schema."""
    pipeline = ds.move(columns=['A', 'C'], pos=1)
    assert pipeline.columns == ['B', 'A', 'C']
    pipeline = pipeline.move(columns=[1, 0], pos=0)
    assert pipeline.columns == ['A', 'B', 'C']
    pipeline = pipeline.move(columns='C', pos=1)
    assert pipeline.columns == ['A', 'C', 'B']
