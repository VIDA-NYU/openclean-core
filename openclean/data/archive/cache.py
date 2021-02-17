# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Implementation of the datastore class that caches dataset snapshots in
memory.
"""

from dataclasses import dataclass
from histore.archive.snapshot import Snapshot
from typing import List, Optional

import pandas as pd

from openclean.data.archive.base import ActionHandle, ArchiveStore
from openclean.data.metadata.base import MetadataStore


@dataclass
class CacheEntry:
    """Entry in a datastore cache. Maintains the data frame and version
    identifier.
    """
    df: pd.DataFrame = None
    version: int = None


class CachedDatastore(ArchiveStore):
    """Wrapper around a datastore that maintains the last dataset version that
    was commited or checked out in main memory. This follows the assumption that
    the spreadsheet view will always display (and modify) this version (and only
    this version).
    """
    def __init__(self, datastore: ArchiveStore):
        """Initialize the reference to the wrapped datastore.

        Parameters
        ----------
        datastore: openclean.data.archive.base.Datastore
            Reference to the datastore that persists the datasets.
        """
        self.datastore = datastore
        self._cache = None

    def checkout(self, version: Optional[int] = None) -> pd.DataFrame:
        """Get a specific version of a dataset. The dataset snapshot is
        identified by the unique version identifier.

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
        # Get the latest dataset version if the argument is None.
        if version is None:
            version = self.datastore.last_version()
        # Serve dataset from cache if present.
        if self._cache is not None:
            # If the requested version matches the cached version return the
            # cached data frame.
            if version == self._cache.version:
                return self._cache.df
        # Dataset has not been caches. Checkout the dataset from the datastore
        # and update the cache.
        df = self.datastore.checkout(version=version)
        self._cache = CacheEntry(df=df, version=version)
        return df

    def commit(self, df: pd.DataFrame, action: Optional[ActionHandle] = None) -> pd.DataFrame:
        """Insert a new version for a dataset. Returns the inserted data frame
        (after potentially modifying the row indexes).

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
        df = self.datastore.commit(df=df, action=action)
        self._cache = CacheEntry(df=df, version=self.datastore.last_version())
        return df

    def last_version(self) -> int:
        """Get a identifier for the last version of the dataset.

        Returns
        -------
        int

        Raises
        ------
        ValueError
        """
        return self.datastore.last_version()

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
        openclean.data.metadata.base.MetadataStore

        Raises
        ------
        ValueError
        """
        return self.datastore.metadata(version=version)

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
        return self.datastore.rollback(version=version)

    def snapshots(self) -> List[Snapshot]:
        """Get list of handles for all versions of a given dataset.

        Returns
        -------
        list of histore.archive.snapshot.Snapshot

        Raises
        ------
        ValueError
        """
        return self.datastore.snapshots()
