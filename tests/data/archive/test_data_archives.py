# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for master data archive manager."""

import os
import pandas as pd
import pytest

from openclean.data.archive.base import Schema

import openclean.data.archive.base as masterdata
import openclean.config as config


DF_1 = pd.DataFrame(data=[[1, 2], [3, 4]], columns=['A', 'B'])
DF_2 = pd.DataFrame(data=[[1, 2], [5, 6]], columns=['A', 'B'])


def test_archive_manager(tmpdir):
    """Test master data manager functionality."""
    # -- Setup ----------------------------------------------------------------
    os.environ[config.ENV_DATA_DIR] = str(tmpdir)
    # Create two new archive for a given dataset.
    masterdata.create('First', source=Schema(['A', 'B']), primary_key=['A'])
    masterdata.create('Second', source=Schema(['A', 'B']), primary_key=['A'])
    archive = masterdata.get('First')
    archive.commit(DF_1)
    archive = masterdata.get('First')
    archive.commit(DF_2)
    archive = masterdata.get('First')
    assert len(archive.snapshots()) == 3
    # -- Re-create archive ----------------------------------------------------
    # Error when providing an existing name without replace.
    with pytest.raises(ValueError):
        masterdata.create('First', source=Schema(['A', 'B']), primary_key=['A'])
    with pytest.raises(ValueError):
        masterdata.create('Second', source=Schema(['A', 'B']), primary_key=['A'])
    archive = masterdata.create('First', source=Schema(['A', 'B']), primary_key=['A'], replace=True)
    archive.commit(DF_1)
    # -- Delete archive -------------------------------------------------------
    with pytest.raises(ValueError):
        masterdata.delete('unknown')
    masterdata.delete('First')
    # Error when deleting or accessing an unknown archive.
    with pytest.raises(ValueError):
        masterdata.delete('First')
    with pytest.raises(ValueError):
        masterdata.get('First')
    # -- Cleanup --------------------------------------------------------------
    del os.environ[config.ENV_DATA_DIR]
