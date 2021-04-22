# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Interfaces and base classes for the data store that is used to maintain all
versions of a data frame.
"""

from abc import ABCMeta, abstractmethod
from histore.archive.base import Archive  # noqa: F401
from histore.archive.manager.base import ArchiveManager  # noqa: F401
from histore.archive.manager.mem import VolatileArchiveManager  # noqa: F401
from histore.archive.manager.persist import PersistentArchiveManager  # noqa: F401
from histore.archive.reader import SnapshotReader
from histore.archive.schema import ArchiveSchema
from histore.archive.snapshot import Snapshot, SnapshotListing  # noqa: F401
from histore.document.base import InputDescriptor as Descriptor  # noqa: F401
from histore.document.mem import Schema  # noqa: F401
from histore.document.operator import DatasetOperator
from typing import Dict, List, Optional, Union

import os
import pandas as pd

from openclean.data.metadata.base import MetadataStore
from openclean.data.stream.base import Datasource

import openclean.config as config


class ActionHandle(metaclass=ABCMeta):
    """Interface for action handles. Defines the serializatio method `to_dict`
    that is used to get a descriptor for the action that created a dataset
    snapshot.
    """
    @abstractmethod
    def to_dict(self) -> Dict:
        """Get a dictionary serialization for the action.

        Returns
        -------
        dict
        """
        raise NotImplementedError()  # pragma: no cover


class ArchiveStore(metaclass=ABCMeta):
    """Interface for the data store that is used to maintain the different
    versions of a dataset that a user creates using the openclean (Jupyter)
    API.
    """
    @abstractmethod
    def apply(
        self, operators: Union[DatasetOperator, List[DatasetOperator]],
        origin: Optional[int] = None, validate: Optional[bool] = None
    ) -> List[Snapshot]:
        """Apply a given operator or a sequence of operators on a snapshot in
        the archive.

        The resulting snapshot(s) will directly be merged into the archive. This
        method allows to update data in an archive directly without the need
        to checkout the snapshot first and then commit the modified version(s).

        Returns list of handles for the created snapshots.

        Note that there are some limitations for this method. Most importantly,
        the order of rows cannot be modified and neither can it insert new rows
        at this point. Columns can be added, moved, renamed, and deleted.

        Parameters
        ----------
        operators: histore.document.operator.DatasetOperator or
                list of histore.document.stream.DatasetOperator
            Operator(s) that is/are used to update the rows in a dataset
            snapshot to create new snapshot(s) in this archive.
        origin: int, default=None
            Unique version identifier for the original snapshot that is being
            updated. By default the last version is updated.
        validate: bool, default=False
            Validate that the resulting archive is in proper order before
            committing the action.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def checkout(self, version: Optional[int] = None) -> pd.DataFrame:
        """Get a specific version of the dataset. The dataset snapshot is
        identified by the unique version identifier.

        Returns the data frame and version number for the dataset snapshot.

        Raises a ValueError if the given version is unknown.

        Parameters
        ----------
        version: int
            Unique dataset version identifier.

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def commit(
        self, source: Datasource, action: Optional[ActionHandle] = None,
        checkout: Optional[bool] = False
    ) -> Datasource:
        """Insert a new dataset snapshot.

        Returns the inserted data frame with potentially modified row indexes.

        Parameters
        ----------
        source: openclean.data.stream.base.Datasource
            Input data frame or stream containing the new dataset version that
            is being stored.
        action: openclean.data.archive.base.ActionHandle, default=None
            Optional handle of the action that created the new dataset version.
        checkout: bool, default=False
            Checkout the commited snapshot and return the result. This option
            is required only if the row index of the given data frame has been
            modified by the commit operation, i.e., if the index of the given
            data frame contained non-integers, negative values, or duplicate
            values.

        Returns
        -------
        openclean.data.stream.base.Datasource
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def last_version(self) -> int:
        """Get the version identifier for the last dataset snapshot.

        Returns
        -------
        int

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def metadata(self, version: Optional[int] = None) -> MetadataStore:
        """Get metadata that is associated with the referenced dataset version.
        If no version is specified the metadata collection for the latest
        version is returned.

        Raises a ValueError if the specified version is unknown.

        Parameters
        ----------
        version: int
            Unique dataset version identifier.

        Returns
        -------
        openclean_.data.metadata.base.MetadataStore

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def open(self, version: Optional[int] = None) -> SnapshotReader:
        """Get a stream reader for a dataset snapshot.

        Parameters
        ----------
        version: int, default=None
            Unique version identifier. By default the last version is used.

        Returns
        -------
        openclean.data.archive.base.SnapshotReader
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def rollback(self, version: int) -> pd.DataFrame:
        """Rollback the archive history to the snapshot with the given version
        identifier.

        Returns the data frame for the napshot that is now the last snapshot in
        the modified archive.

        Parameters
        ----------
        version: int
            Unique identifier of the rollback version.

        Returns
        -------
        pd.DataFrame
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def schema(self) -> ArchiveSchema:
        """Get the schema history for the archived dataset.

        Returns
        -------
        openclean.data.archive.base.ArchiveSchema
        """
        raise NotImplementedError()  # pragma: no cover

    @abstractmethod
    def snapshots(self) -> List[Snapshot]:
        """Get list of handles for all versions of the dataset.

        Returns
        -------
        list of histore.archive.snapshot.Snapshot

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()  # pragma: no cover


# -- Archive Functions --------------------------------------------------------


def create(
    dataset: str, source: Optional[Datasource], primary_key: Optional[List[str]],
    replace: Optional[bool] = False
) -> Archive:
    """Create a new archive for a dataset with the given identifier. If an
    archive with the given identifier exists it will be replaced (if the
    replace flag is True) or an error is raised.

    Parameters
    ----------
    dataset: string
        Unique dataset identifier.
    source: openclean.data.archive.base.Datasource, default=None
        Initial dataset snapshot that is loaded into the created archive.
    primary_key: list of string
        List of primary key attributes for merging snapshots into the created
        archive.
    replace: bool, default=False
        Replace an existing archive with the same name if it exists.

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
        doc=source,
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
    return PersistentArchiveManager(basedir=os.path.join(config.DATADIR(), 'archives'))
