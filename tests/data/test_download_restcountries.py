# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import pytest

from openclean.data.downloader.restcountries import RestcountriesDownloader

import openclean.data.downloader.restcountries as restcountries


def test_download_restcountries():
    """Test downloading the restcountries project dataset."""
    downloader = RestcountriesDownloader()
    # Get column names from descriptor
    datasets = downloader.datasets()
    assert len(datasets) == 1
    ds = datasets[0]
    assert ds.identifier() == restcountries.COUNTRIES
    columns = [c.name() for c in ds.columns()]
    assert columns == [
            'name',
            'alpha2Code',
            'alpha3Code',
            'capital',
            'region',
            'subregion'
        ]
    downloads = downloader.download()
    assert len(downloads) == 1
    df = downloads[restcountries.COUNTRIES]
    assert list(df.columns) == columns
    with pytest.raises(ValueError):
        downloader.download(datasets='capitals')
