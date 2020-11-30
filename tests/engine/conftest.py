# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the HISTORE-based implementation of the data store."""

import pandas as pd
import pytest

from openclean.engine.base import DB


@pytest.fixture
def dataset():
    """Returns a basic data frame with three columns and two rows."""
    return pd.DataFrame(data=[[1, 2, 3], [3, 4, 5]], columns=['A', 'B', 'C'])


@pytest.fixture
def persistent_engine(tmpdir):
    """Create a new instance of the Openclean engine with a persistent HISTORE
    data store as the backend.
    """
    return DB(basedir=str(tmpdir), create=True)


@pytest.fixture
def volatile_engine():
    """Create a new instance of the Openclean engine with a volatile HISTORE
    data store as the backend.
    """
    return DB()
