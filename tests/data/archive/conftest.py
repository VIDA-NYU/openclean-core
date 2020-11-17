# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Fixtures for datastore unit tests."""

import pandas as pd
import pytest

from histore import PersistentArchiveManager

from openclean.data.archive.histore import HISTOREDatastore


@pytest.fixture
def dataset():
    """Returns a basic data frame with three columns and two rows."""
    return pd.DataFrame(data=[[1, 2, 3], [3, 4, 5]], columns=['A', 'B', 'C'])


@pytest.fixture
def store(tmpdir):
    """Create a new instance of the HISTORE data store."""
    manager = PersistentArchiveManager(basedir=str(tmpdir))
    archive = manager.get(manager.create('test').identifier())
    return HISTOREDatastore(archive=archive)
