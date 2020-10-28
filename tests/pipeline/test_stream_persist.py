# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the data pipeline persist operators."""

import os

from openclean.function.eval.base import Col


def test_persist_stream_in_file(ds, tmpdir):
    """Test persisting a data stream on disk."""
    filename = os.path.join(tmpdir, 'data.csv')
    ds = ds.filter(Col('B') >= 5).persist(filename=filename)
    assert ds.pipeline == []
    assert ds.count() == 5


def test_persist_stream_in_memory(ds):
    """Test persisting a data stream for future processing."""
    ds = ds.filter(Col('B') >= 5).persist()
    assert ds.pipeline == []
    assert ds.count() == 5
