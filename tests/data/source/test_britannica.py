# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Encyclopaedia Britannica repository."""

import csv
import os
import pytest

from openclean.data.source.britannica import EncyclopaediaBritannica

import openclean.data.source.britannica as britannica


"""Schema for the US cities dataset."""
SCHEMA = ['city', 'state']


@pytest.fixture
def repo():
    """Fixture to get the Encyclopaedia Britannica repository handle."""
    return EncyclopaediaBritannica()


def test_britannica_catalog(repo):
    """Test catalog of datasets for the Encyclopaedia Britannica."""
    # Get list of all datasets.
    datasets = list(repo.catalog())
    # There should only be one entry.
    assert len(datasets) == 1
    assert datasets[0].identifier == britannica.US_CITIES
    # -- Get the dataset handle -----------------------------------------------
    ds = repo.dataset(britannica.US_CITIES)
    assert [c.name for c in ds.columns] == SCHEMA
    # -- Error when accessing invalid dataset. --------------------------------
    with pytest.raises(ValueError):
        repo.dataset('UNKNOWN')


def test_restcountries_load(repo):
    """Test loading the US cities dataset as a data frame."""
    df = repo.dataset(britannica.US_CITIES).load()
    assert list(df.columns) == SCHEMA


def test_restcountries_write(repo, tmpdir):
    """Test writing the US cities dataset to a tab-delimited file."""
    filename = os.path.join(tmpdir, 'cities.tsv')
    with open(filename, 'w') as f:
        repo.dataset(britannica.US_CITIES).write(f)
    with open(filename, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        assert next(reader) == SCHEMA
