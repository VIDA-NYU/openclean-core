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
from histore.archive.snapshot import Snapshot
from histore.document.csv.base import CSVFile
from typing import Dict, List, Optional, Tuple

import pandas as pd

from openclean.data.metadata.base import MetadataStore


"""Type aliases for API methods."""
# Data sources for loading are either pandas data frames or references to files.
Datasource = Tuple[pd.DataFrame, CSVFile, str]


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
    def commit(self, df: pd.DataFrame, action: Optional[ActionHandle] = None) -> pd.DataFrame:
        """Insert a new dataset snapshot.

        Returns the inserted data frame (after potentially modifying the row
        indexes) and the version identifier for the commited version.

        Parameters
        ----------
        df: pd.DataFrame
            Data frame containing the new dataset version that is being stored.
        action: openclean.data.archive.base.ActionHandle, default=None
            Optional handle of the action that created the new dataset version.

        Returns
        -------
        pd.DataFrame
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
