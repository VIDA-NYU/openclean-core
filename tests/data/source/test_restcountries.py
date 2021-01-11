# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the restcountries project repository."""

import csv
import os
import pytest

from openclean.data.source.restcountries import RestcountriesRepository

import openclean.data.source.restcountries as restcountries


"""Schema for the countries of the world dataset."""
SCHEMA = ['name', 'alpha2Code', 'alpha3Code', 'capital', 'region', 'subregion']


@pytest.fixture
def repo():
    """Fixture to get the restcountries project repository handle."""
    return RestcountriesRepository()


def test_restcountries_catalog(repo):
    """Test catalog of datasets for the restcountries project."""
    # Get list of all datasets.
    datasets = list(repo.catalog())
    # There should only be one entry.
    assert len(datasets) == 1
    assert datasets[0].identifier == restcountries.COUNTRIES
    # -- Get the dataset handle -----------------------------------------------
    ds = repo.dataset(restcountries.COUNTRIES)
    assert [c.name for c in ds.columns] == SCHEMA
    # -- Error when accessing invalid dataset. --------------------------------
    with pytest.raises(ValueError):
        repo.dataset('UNKNOWN')


def test_restcountries_load(repo):
    """Test loading the restcountries dataset as a data frame."""
    df = repo.dataset(restcountries.COUNTRIES).load()
    assert list(df.columns) == SCHEMA


def test_restcountries_write(repo, tmpdir):
    """Test writing the restcountries dataset to a tab-delimited file."""
    filename = os.path.join(tmpdir, 'contries.tsv')
    with open(filename, 'w', encoding='utf-8') as f:
        repo.dataset(restcountries.COUNTRIES).write(f)
    with open(filename, 'rt') as f:
        reader = csv.reader(f, delimiter='\t')
        assert next(reader) == SCHEMA
