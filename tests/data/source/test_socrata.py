# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the Socrata Open Data API repository."""

import csv
import json
import os
import pytest
import requests

from openclean.data.source.socrata import Socrata


"""Identifier for the 'DOHMH Farmers Markets' dataset that is downloaded for
test purposes.
"""
DATASET = '8vwk-6iz2'

"""Downloaded files for various test routes to avoid having to read data via
HTTP requests during unit testing.
"""
DIR = os.path.dirname(os.path.realpath(__file__))
DATA_DIR = os.path.join(DIR, './.files')

DATASET_FILE = os.path.join(DATA_DIR, 'DOHMH_Farmers_Markets.tsv')
DESCRIPTOR_FILE = os.path.join(DATA_DIR, '8vwk-6iz2.json')
DOMAIN_EU_FILE = os.path.join(DATA_DIR, 'api.eu.socrata.com.json')
DOMAIN_US_FILE = os.path.join(DATA_DIR, 'api.us.socrata.com.json')
NYCCATALOG_FILE = os.path.join(DATA_DIR, 'data.cityofnewyork.us.json')


# -- Fixtures -----------------------------------------------------------------

class MockResponse:
    """Mock response object for API requests. Adopted from the online documentation
    at: https://docs.pytest.org/en/stable/monkeypatch.html
    """
    def __init__(self, url, headers=None):
        """Keep track of the request Url, and the optional request body and
        headers.
        """
        self.url = url
        self.status_code = 404 if url.endswith('ids=UNKNOWN') else 200
        self._fh = None

    def __enter__(self):
        self._fh = open(DATASET_FILE, 'rb')
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._fh.close()

    @property
    def content(self):
        """Raw response for file downloads."""
        with open(DATASET_FILE, 'rb') as f:
            return f.read()

    def iter_content(self, chunk_size):
        while True:
            data = self._fh.read(chunk_size)
            if not data:
                break
            yield data

    def json(self):
        """Return dictionary containing data depending on the given Url."""
        if self.url == 'http://api.us.socrata.com/api/catalog/v1/domains':
            filename = DOMAIN_US_FILE
        elif self.url == 'http://api.eu.socrata.com/api/catalog/v1/domains':
            filename = DOMAIN_EU_FILE
        elif self.url == 'http://api.us.socrata.com/api/catalog/v1?ids=8vwk-6iz2':
            filename = DESCRIPTOR_FILE
        elif self.url == 'http://api.us.socrata.com/api/catalog/v1?domains=data.cityofnewyork.us&only=datasets&limit=10000':
            filename = NYCCATALOG_FILE
        with open(filename, 'r') as f:
            return json.load(f)

    def raise_for_status(self):
        """Never raise error for failed requests."""
        pass


@pytest.fixture
def mock_response(monkeypatch):
    """Requests.get() mocked to return {'mock_key':'mock_response'}."""

    def mock_get(*args, **kwargs):
        return MockResponse(*args)

    def mock_post(*args, **kwargs):
        return MockResponse(*args, **kwargs)

    monkeypatch.setattr(requests, "get", mock_get)


# -- Unit tests ---------------------------------------------------------------

def test_socrata_catalog(mock_response):
    """Test getting a list of datasets for domain 'data.cityofnewyork.us'."""
    repo = Socrata()
    count = 0
    for ds in repo.catalog('data.cityofnewyork.us'):
        count += 1
    assert count > 0
    # -- Error when accessing invalid dataset. --------------------------------
    with pytest.raises(ValueError):
        repo.dataset('UNKNOWN')


def test_socrata_domains(mock_response):
    """Test getting a list of domain names from the Socrata Open Data API."""
    repo = Socrata(app_token='0000')
    domains = repo.domains()
    assert len(domains) > 0
    assert 'data.cityofnewyork.us' in [d for _, d in domains]


def test_dataset_load(mock_response):
    """Test loading s Socrata dataset as a data frame."""
    repo = Socrata()
    df = repo.dataset(DATASET).load()
    assert len(df.columns) > 0


def test_dataset_write(mock_response, tmpdir):
    """Test writing a Socrata dataset to a tab-delimited file."""
    repo = Socrata()
    filename = os.path.join(tmpdir, 'data.tsv')
    with open(filename, 'wb') as f:
        repo.dataset(DATASET).write(f)
    with open(filename, 'rt') as f:
        reader = csv.reader(f, delimiter='\t')
        assert len(next(reader)) > 0
