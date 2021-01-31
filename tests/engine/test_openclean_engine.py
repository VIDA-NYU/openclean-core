# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the basic functionality of the openclean engine."""

import pytest

from openclean.engine.base import DB


@pytest.mark.parametrize('cached', [True, False])
def test_recreate_engine(cached, dataset, tmpdir):
    """Test creating and deleting datasets via the engine and accessing them
    after re-creating the engine.
    """
    engine = DB(basedir=str(tmpdir), create=True)
    # Create two datasets with a single snapshot.
    df = engine.create(source=dataset, name='My Data')
    assert df.shape == (2, 3)
    df = engine.create(source=df[df['A'] == 1], name='More Data')
    assert df.shape == (1, 3)
    # Re-create the engine and access the snapshots.
    engine = DB(basedir=str(tmpdir), create=False, cached=cached)
    df = engine.checkout(name='My Data')
    assert df.shape == (2, 3)
    df = engine.checkout(name='More Data')
    assert df.shape == (1, 3)
