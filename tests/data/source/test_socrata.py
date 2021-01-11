# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Socrata Open Data API repository."""

import csv
import os
import pytest

from openclean.data.source.socrata import Socrata


"""Identifier for the 'DOHMH Farmers Markets' dataset that is downloaded for
test purposes.
"""
DATASET = '8vwk-6iz2'


@pytest.fixture
def repo():
    """Fixture to get the Socrata Open Data API repository handle."""
    return Socrata()


def test_socrata_catalog(repo):
    """Test getting a list of datasets for domain 'data.cityofnewyork.us'."""
    count = 0
    for ds in repo.catalog('data.cityofnewyork.us'):
        count += 1
    assert count > 0
    # -- Error when accessing invalid dataset. --------------------------------
    with pytest.raises(ValueError):
        repo.dataset('UNKNOWN')


def test_socrata_domains(repo):
    """Test getting a list of domain names from the Socrata Open Data API."""
    domains = repo.domains()
    assert len(domains) > 0
    assert 'data.cityofnewyork.us' in [d for _, d in domains]


def test_dataset_load(repo):
    """Test loading the restcountries dataset as a data frame."""
    df = repo.dataset(DATASET).load()
    assert len(df.columns) > 0


def test_dataset_write(repo, tmpdir):
    """Test writing the restcountries dataset to a tab-delimited file."""
    filename = os.path.join(tmpdir, 'data.tsv')
    with open(filename, 'wb') as f:
        repo.dataset(DATASET).write(f)
    with open(filename, 'rt') as f:
        reader = csv.reader(f, delimiter='\t')
        assert len(next(reader)) > 0
