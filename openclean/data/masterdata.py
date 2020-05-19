# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Master data downloader."""

import json
import requests

from openclean.data.masterdata.store import repositories

import openclean.config as config


def download(repository, datasets=None, properties=None, basedir=None):
    """Download datasets from a given repository to the local master data
    store. Raises a ValueError if the repository identifier is unknown.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    datasets: string or list of string, default=None
        List of identifier for datasets that are downladed. The set of valid
        identifier is depending on the repository.
    properties: dict, default=None
        Additional properties for the download. The set of valid properties is
        dependent on the repository.
    basedir: string, default=None
        Target directory for downloaded repositories.

    Raises
    ------
    ValueError
    """
    # Get the download directory from the environment if not given.
    if basedir is None:
        basedir = config.MASTERDATADIR()
    # Get the repository handle from the registered repositories.
    rh = repositories().get(repository)
    if rh is None:
        raise ValueError('unknown repository {}'.format(repository))
    # Get the loader class instance
    downloads = rh.download(datasets=datasets, properties=properties)
    for dataset_id in downloads:
        pass


def load(filename):
    if filename == 'restcountries.eu':
        return load_restcountries_eu()
    filename = os.path.join(config.DATA_DIR(), filename)
    return dataset(filename)




def domain(repository, columns=None, download_dir=None):
    """Get set of values in the master data repository with the given
    identifier. If the repository is structured, a list of columns can be
    specified to further select values from the repository.

    Raises a ValueError if the repository is unknown.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    columns: string or list(string)
        Columns in the repository whose values are returned.
    download_dir: sreing, optional
        Source directory where the repository file has been downloaded.

    Returns
    -------
    set

    Raises
    ------
    ValueError
    """
    # Ensure that columns is a list (if given).
    if columns is not None:
        if not isinstance(columns, list):
            columns = [columns]
    # Get the download directory from the environment if not given.
    if download_dir is None:
        download_dir = config.MASTERDATA_DIR()
    if repository == 'restcountries.eu':
        filename = os.path.join(download_dir, 'restcountries.eu.json')
        return read_countries_eu(filename, columns=columns)
    raise ValueError('unknown repository {}'.format(repository))


def mapping(repository, source, target, download_dir=None):
    """Get set of values in the master data repository with the given
    identifier. If the repository is structured, a list of columns can be
    specified to further select values from the repository.

    Raises a ValueError if the repository is unknown.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    columns: string or list(string)
        Columns in the repository whose values are returned.
    download_dir: sreing, optional
        Source directory where the repository file has been downloaded.

    Returns
    -------
    set

    Raises
    ------
    ValueError
    """
    # Get the download directory from the environment if not given.
    if download_dir is None:
        download_dir = config.MASTERDATA_DIR()
    if repository == 'restcountries.eu':
        filename = os.path.join(download_dir, 'restcountries.eu.json')
        return read_countries_eu_mapping(filename, source, target)
    raise ValueError('unknown repository {}'.format(repository))


# -- Helper methods -----------------------------------------------------------

def read_countries_eu(filename, columns):
    """Get set of values from the downloaded restcountries project data for the
    given column (or list of columns).

    Parameters
    ----------
    filename: string
        Path to downloaded repository file.
    columns: string or list(string)
        Columns in the repository whose values are returned.

    Returns
    -------
    set
    """
    with open(filename, 'r') as f:
        doc = json.load(f)
    values = set()
    for obj in doc:
        if len(columns) == 1:
            values.add(obj[columns[0]])
        else:
            values.add(tuple([obj[c] for c in columns]))
    return values


def read_countries_eu_mapping(filename, source, target):
    """Get set of values from the downloaded restcountries project data for the
    given column (or list of columns).

    Parameters
    ----------
    filename: string
        Path to downloaded repository file.
    columns: string or list(string)
        Columns in the repository whose values are returned.

    Returns
    -------
    set
    """
    with open(filename, 'r') as f:
        doc = json.load(f)
    values = dict()
    for obj in doc:
        values[obj[source]] = obj[target]
    return values
