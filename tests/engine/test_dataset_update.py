# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for updating datasets via the openclean engine."""


def test_dataset_update(persistent_engine, dataset):
    """Test updates to a given dataset and retrieving all dataset versions."""
    persistent_engine.load_dataset(source=dataset, name='my_dataset', primary_key='A')
    persistent_engine.dataset('my_dataset').update('B', 1)
    persistent_engine.dataset('my_dataset').update('C', 2)
    # Get version history.
    snapshots = persistent_engine.dataset('my_dataset').snapshots()
    assert len(snapshots) == 3
    # Version 1
    df = persistent_engine.checkout('my_dataset', version=0)
    assert list(df['A']) == [1, 3]
    assert list(df['B']) == [2, 4]
    assert list(df['C']) == [3, 5]
    # Version 2
    df = persistent_engine.checkout('my_dataset', version=1)
    assert list(df['A']) == [1, 3]
    assert list(df['B']) == [1, 1]
    assert list(df['C']) == [3, 5]
    # Version 3
    df = persistent_engine.checkout('my_dataset', version=2)
    assert list(df['A']) == [1, 3]
    assert list(df['B']) == [1, 1]
    assert list(df['C']) == [2, 2]
