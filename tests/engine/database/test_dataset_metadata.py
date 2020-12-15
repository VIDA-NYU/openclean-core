# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for metadata that is maintained together with dataset snapshots in
the openclean engine.
"""

import pandas as pd
import pytest

from histore.archive.manager.mem import VolatileArchiveManager

from openclean.engine.base import OpencleanEngine
from openclean.engine.library import ObjectLibrary


@pytest.fixture
def dataset():
    """Returns a basic data frame with three columns and two rows."""
    return pd.DataFrame(data=[[1, 2, 3], [3, 4, 5]], columns=['A', 'B', 'C'])


@pytest.fixture
def engine(tmpdir):
    """Get a openclean engine."""
    engine = OpencleanEngine(
        identifier='0000',
        manager=VolatileArchiveManager(),
        library=ObjectLibrary(),
        basedir=tmpdir,
        cached=True
    )
    return engine


def test_dataset_metadata(engine, dataset):
    """Test accessing snapshot metadata for a dataset via the openclean engine."""
    engine.create(source=dataset, name='DS')
    engine.metadata('DS').set_annotation(key='A', value=1)
    assert engine.metadata('DS').get_annotation(key='A') == 1
    engine.dataset('DS').update(columns='A', func=0)
    engine.metadata('DS').set_annotation(key='A', value=2)
    assert engine.metadata('DS').get_annotation(key='A') == 2
    with pytest.raises(ValueError):
        engine.metadata('UNKNOWN')
