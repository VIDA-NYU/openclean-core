# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Collection of functionts that wrap araound the `histore` package for
managing archives of datasets.
"""


from typing import List, Optional

from histore import PersistentArchiveManager
from histore.archive.base import Archive

import openclean.config as config


def create(
    dataset: str, primary_key: Optional[List[str]], replace: Optional[bool] = False
) -> Archive:
    """Create a new archive for a dataset with the given identifier. If an
    archive with the given identifier exists it will be replaced (if the
    replace flag is True) or an error is raised.

    Parameters
    ----------
    dataset: string
        Unique dataset identifier.

    Returns
    -------
    histore.archive.base.Archive

    Raises
    ------
    ValueError
    """
    archives = manager()
    # Check if an archive with the given identifier exists.
    for descriptor in archives.list():
        if descriptor.name() == dataset:
            if replace:
                archives.delete(descriptor.identifier())
                break
            raise ValueError("dataset '{}' exists".format(dataset))
    # Create a new archive ad return the archive handle.
    descriptor = archives.create(
        name=dataset,
        primary_key=primary_key
    )
    return archives.get(descriptor.identifier())


def delete(dataset: str):
    """Delete the existing archive for a dataset. Raises a ValueError if the
    dataset is unknown.

    Parameters
    ----------
    dataset: string
        Unique dataset identifier.

    Raises
    ------
    ValueError
    """
    # Get the master data manager.
    archives = manager()
    # Get the existing archive for the dataset. Raises a ValueError if the
    # dataset is unknown.
    for descriptor in archives.list():
        if descriptor.name() == dataset:
            archives.delete(descriptor.identifier())
            return
    raise ValueError("unknown dataset '{}'".format(dataset))


def get(dataset: str) -> Archive:
    """Get the existing archive for a dataset. Raises a ValueError if the
    dataset is unknown.

    Parameters
    ----------
    dataset: string
        Unique dataset identifier.

    Returns
    -------
    histore.archive.base.Archive

    Raises
    ------
    ValueError
    """
    # Get the master data manager.
    archives = manager()
    # Get the existing archive for the dataset. Raises a ValueError if the
    # dataset is unknown.
    for descriptor in archives.list():
        if descriptor.name() == dataset:
            return archives.get(descriptor.identifier())
    raise ValueError("unknown dataset '{}'".format(dataset))


def manager() -> PersistentArchiveManager:
    """Get instance of the archive manager that is used to maintain master
    datasets.

    Returns
    -------
    histore.archive.manager.base.ArchiveManager
    """
    return PersistentArchiveManager(basedir=config.MASTERDATADIR())
