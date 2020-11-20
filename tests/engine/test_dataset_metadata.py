# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for metadata that is maintained together with dataset snapshots in
the openclean engine.
"""

import pytest


def test_dataset_metadata(persistent_engine, dataset):
    """Test accessing snapshot metadata for a dataset via the openclean engine."""
    persistent_engine.create(source=dataset, name='DS')
    persistent_engine.metadata('DS').set_annotation(key='A', value=1)
    persistent_engine.dataset('DS').update(columns='A', func=0)
    persistent_engine.metadata('DS').set_annotation(key='A', value=2)
    assert persistent_engine.metadata('DS', version=0).get_annotation(key='A') == 1
    assert persistent_engine.metadata('DS', version=1).get_annotation(key='A') == 2
    with pytest.raises(ValueError):
        persistent_engine.metadata('UNKNOWN')
