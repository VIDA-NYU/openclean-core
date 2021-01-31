# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Unit tests for the integration of the reference data store in openclean."""

from io import BytesIO
from refdata.config import ENV_BASEDIR

import os
import pytest
import requests

import openclean.data.refdata as refdata


# -- Patch for data file requests ---------------------------------------------

INDEX_JSON = {
    'datasets': [
        {
            'id': 'test',
            'url': 'http://test.me',
            "checksum": "62b32aeded8f7f29de45f9ce3683811a8136725723170764b092ffa607acdfdf",
            'format': {'type': 'csv', 'parameters': {}},
            'schema': [
                {'id': 'C1'},
                {'id': 'C2'}
            ]
        }
    ]
}


class MockResponse:
    """Mock response object for requests to download the repository index.
    Adopted from the online documentation at:
    https://docs.pytest.org/en/stable/monkeypatch.html
    """
    def __init__(self, url):
        """Keep track of the request Url to be able to load different test
        data files.
        """
        self._fh = None

    def __enter__(self):
        self._fh = BytesIO(b'C1,C2\nv1,v2')
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._fh = None

    def iter_content(self, chunk_size):
        self.__enter__()
        if self._fh is not None:
            yield self._fh.read()
            self._fh = None
        self.__exit__(None, None, None)

    def json(self):
        """If the Url is the DEFAULT_URL the test index is returned. Otherwise
        an attempt is made to read the index data file.
        """
        return INDEX_JSON

    def raise_for_status(self):
        """Never raise an error for a failed requests."""
        pass


@pytest.fixture
def mock_response(monkeypatch):
    """Requests.get() mocked to return index document."""

    def mock_get(*args, **kwargs):
        return MockResponse(*args)

    monkeypatch.setattr(requests, "get", mock_get)


# -- Unit tests ---------------------------------------------------------------

def test_reference_file_lifecycle(mock_response, tmpdir):
    """Test life cyle for reference data files: download, list, get, remove."""
    # -- Setup ----------------------------------------------------------------
    os.environ[ENV_BASEDIR] = str(tmpdir)
    # -- Tests ----------------------------------------------------------------
    assert len(refdata.repository()) == 1
    assert not refdata.list()
    refdata.download(key='test')
    assert len(refdata.list()) == 1
    fh = refdata.open(key='test')
    assert fh.identifier == 'test'
    refdata.remove(key='test')
    assert not refdata.list()
    # -- Cleanup --------------------------------------------------------------
    del os.environ[ENV_BASEDIR]
