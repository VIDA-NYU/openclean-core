# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the rename operator in data processing pipelines."""


def test_rename_columns_in_stream(ds):
    """Test renaming columns in the data stream schema."""
    pipeline = ds.rename(columns=['A', 'C'], names=['E', 'F'])
    assert pipeline.columns == ['E', 'B', 'F']
    pipeline = pipeline.rename(columns=[1, 0], names=['G', 'H'])
    assert pipeline.columns == ['H', 'G', 'F']
    pipeline = pipeline.rename(columns='G', names='K')
    assert pipeline.columns == ['H', 'K', 'F']
