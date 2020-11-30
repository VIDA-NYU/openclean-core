# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for editing a dataset in the spreadsheet view."""

from openclean.engine.base import DB


def test_sample_dataset(dataset, tmpdir):
    """Test to ensure that we can call the edit function without an error."""
    engine = DB(str(tmpdir))
    engine.create(source=dataset, name='DS', primary_key='A')
    df = engine.sample('DS', n=1)
    assert df.shape == (1, 3)
