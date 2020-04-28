# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Master data downloader."""

import json
import os
import requests

import openclean.config as config


def download(repository, download_dir=None):
    """Download the repository with the given identifier to the local master
    data store. Raises a ValueError if the repository identifier is unknown.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    download_dir: sreing, optional
        Target directory for the downloaded file

    Raises
    ------
    ValueError
    """
    # Get the download directory from the environment if not given.
    if download_dir is None:
        download_dir = config.DOWNLOAD_DIR()
    if repository == 'restcountries.eu':
        filename = os.path.join(download_dir, 'restcountries.eu.json')
        download_countries_eu(filename)
    else:
        raise ValueError('unknown repository {}'.format(repository))


# -- Helper methods -----------------------------------------------------------

def download_countries_eu(filename):
    """Download the full list of country information from the restcountries
    project API. The file is a Json file that will be stored under the given
    filename.

    Parameters
    ----------
    filename: string
        Path to file on disk where the downloaded data will be stored.

    Raises
    ------
    HttpError
    """
    # Fetch data from API endpoint.
    r = requests.get('https://restcountries.eu/rest/v2/all')
    r.raise_for_status()
    with open(filename, 'w') as f:
        json.dump(r.json(), f)
