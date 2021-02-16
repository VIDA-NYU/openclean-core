# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Module that provides access to reference data sets that are downloaded from
a reference data repository to the local file system.
"""

from typing import IO, List, Optional, Set, Union

from refdata.base import DatasetDescriptor
from refdata.dataset.base import DatasetHandle
from refdata.repo.loader import RepositoryIndexLoader
from refdata.store.base import LocalStore as LocalStore  # noqa: F401

from openclean.version import __version__


# -- Reference data store for openclean ---------------------------------------

class RefStore(LocalStore):
    """Default local store for the openclean package. Uses the module name and
    package version to set the respective properties of the created local store
    instance.
    """
    def __init__(
        self, basedir: Optional[str] = None, loader: Optional[RepositoryIndexLoader] = None,
        auto_download: Optional[bool] = None, connect_url: Optional[str] = None
    ):
        """Initialize the store properties.

        Parameters
        ----------
        basedir: string, default=None
            Path to the directory for downloaded datasets. By default, the
            directory that is specified in the environment variable REFDATA_BASEDIR
            is used. If the environment variable is not set an directory under
            the OS-specific users cache data directory is used.
        loader: refdata.repo.loader.RepositoryIndexLoader, default=None
            Loader for a dataset repository index. the loaded index is used to
            create an instance of the repository manager that is associated with
            the local data store for downloading datasets.
        auto_download: bool, default=None
            If auto download is enabled (True) datasets are downloaded automatically
            when they are first accessed via `.open()`. If this option is not
            enabled and an attempt is made to open a datasets that has not yet
            been downloaded to the local file syste, an error is raised. If this
            argument is not given the value from the environment variable
            REFDATA_AUTODOWNLOAD is used or False if the variable is not set.
        connect_url: string, default=None
            SQLAlchemy database connect Url string. If a value is given it is
            assumed that the database exists and has been initialized. If no
            value is given the default SQLite database is used. If the respective
            database file does not exist a new database will be created.
        """
        super(RefStore, self).__init__(
            package_name=__name__.split('.')[0],
            package_version=__version__,
            basedir=basedir,
            loader=loader,
            auto_download=auto_download,
            connect_url=connect_url
        )


# -- Shortcuts to maintain and access reference data files --------------------

def download(key: str):
    """Download the file with the given unique identifier to the local reference
    data store.

    Parameters
    ----------
    key: string
        Unique reference data file identifier.
    """
    store().download(key=key)


def list() -> List[DatasetDescriptor]:
    """Get the descriptors for all datasets that have been downloaded and
    are available from the local dataset store.

    Returns
    -------
    list of refdata.base.DatasetDescriptor
    """
    return store().list()


def load(key: str, auto_download: Optional[bool] = None) -> DatasetHandle:
    """Get a handle for the dataset with the given unique identifier.

    If the dataset has not been downloaded to the local store yet, it will be
    downloaded if the `auto_download` flag is True or if the environment
    variable `REFDATA_AUTODOWNLOAD` is set to True. The `auto_download`
    parameter for this function will override the value in the environment
    variable when loading the dataset.

    If the dataset is not available in the local store (and is not downloaded
    automatically) an error is raised.

    Parameters
    ----------
    key: string
        External unique dataset identifier.
    auto_download: bool, default=None
        Override the class global auto download flag.

    Returns
    -------
    refdata.dataset.DatasetHandle

    Raises
    ------
    refdata.error.NotDownloadedError
    """
    return store().load(key=key, auto_download=auto_download)


def open(key: str, auto_download: Optional[bool] = None) -> IO:
    """Open the data file for the dataset with the given unique identifier.

    If the dataset has not been downloaded to the local store yet, it will be
    downloaded if the `auto_download` flag is True or if the environment
    variable `REFDATA_AUTODOWNLOAD` is set to True. The `auto_download`
    parameter for this function will override the value in the environment
    variable when opening the dataset.

    If the dataset is not available in the local store (and is not downloaded
    automatically) an error is raised.

    Parameters
    ----------
    key: string
        External unique dataset identifier.
    auto_download: bool, default=None
        Override the class global auto download flag.

    Returns
    -------
    file-like object

    Raises
    ------
    refdata.error.NotDownloadedError
    """
    return store().open(key=key, auto_download=auto_download)


def remove(key: str) -> bool:
    """Remove the dataset with the given unique identifier from the local
    store. Returns True if the dataset was removed and False if the dataset
    had not been downloaded before.

    Parameters
    ----------
    key: string
        External unique dataset identifier.

    Returns
    -------
    bool
    """
    return store().remove(key=key)


def repository(filter: Optional[Union[str, List[str], Set[str]]] = None) -> List[DatasetDescriptor]:
    """Query the repository index that is associated with the local reference
    data store.

    The filter is a single tag or a list of tags. The result will include those
    datasets that contain all the query tags. The search includes the dataset
    tags as well a the tags for individual dataset columns.

    If no filter is specified the full list of datasets descriptors in the
    repository is returned.

    Parameters
    ----------
    filter: string, list of string, or set of string
        (List of) query tags.

    Returns
    -------
    list of refdata.base.DatasetDescriptor
    """
    return store().repository().find(filter=filter)


def store() -> RefStore:
    """Get an instance of the local reference data store.

    This function is used by other function sin this module to create the local
    reference data store. By now we use all the default settings when creating
    the data store. In the future we may want to use a openclean-specific
    configuration instead.

    Returns
    -------
    openclean.data.refdata.RefStore
    """
    return RefStore()
