# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for master data archive manager."""

import os
import pandas as pd
import pytest

import openclean.data.masterdata as masterdata
import openclean.config as config


DF_1 = pd.DataFrame(data=[[1, 2], [3, 4]], columns=['A', 'B'])
DF_2 = pd.DataFrame(data=[[1, 2], [5, 6]], columns=['A', 'B'])


def test_masterdata_manager(tmpdir):
    """Test master data manager functionality."""
    # -- Setup ----------------------------------------------------------------
    os.environ[config.ENV_MASTERDATA_DIR] = str(tmpdir)
    # Create new archive for a given dataset.
    archive = masterdata.create('MYDS', primary_key=['A'])
    archive.commit(DF_1)
    archive = masterdata.get('MYDS')
    archive.commit(DF_2)
    archive = masterdata.get('MYDS')
    assert len(archive.snapshots()) == 2
    # -- Re-create archive ----------------------------------------------------
    # Error when providing an existing name without replace.
    with pytest.raises(ValueError):
        masterdata.create('MYDS', primary_key=['A'])
    archive = masterdata.create('MYDS', primary_key=['A'], replace=True)
    archive.commit(DF_1)
    # -- Delete archive -------------------------------------------------------
    masterdata.delete('MYDS')
    # Error when deleting or accessing an unknown archive.
    with pytest.raises(ValueError):
        masterdata.delete('MYDS')
    with pytest.raises(ValueError):
        masterdata.get('MYDS')
    # -- Cleanup --------------------------------------------------------------
    del os.environ[config.ENV_MASTERDATA_DIR]
