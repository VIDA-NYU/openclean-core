# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Implementation of the data store that is based on HISTORE. For each dataset
a new HISTORE archive will be maintained. This archive is augmented with
storage of dataset metadata.
"""

from typing import List, Optional
from histore.archive.snapshot import Snapshot

import pandas as pd

from histore.archive.base import Archive

from openclean.data.archive.base import ActionHandle, ArchiveStore
from openclean.data.metadata.base import MetadataStore, MetadataStoreFactory
from openclean.data.metadata.mem import VolatileMetadataStoreFactory


class HISTOREDatastore(ArchiveStore):
    """Data store implementation that is based on HISTORE. This class is a
    simple wrapper around a HISTORE archive.
    """
    def __init__(self, archive: Archive, metastore: Optional[MetadataStoreFactory] = None):
        """Initialize the base directory for archive metadata and the reference
        to the archive.

        Parameters
        ----------
        archive: histore.archive.base.Archive
            Archive for dataset snapshots.
        metastore: openclean.data.metadata.base.MetadataStoreFactory, default=None
            Factory for snapshot metadata stores.
        """
        self.archive = archive
        self.metastore = metastore if metastore is not None else VolatileMetadataStoreFactory()
        # Maintain a reference to the snapshot for the last version
        self._last_snapshot = archive.snapshots().last_snapshot()

    def checkout(self, version: Optional[int] = None) -> pd.DataFrame:
        """Get a specific dataset snapshot. The snapshot is identified by
        the unique version identifier.

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
        return self.archive.checkout(version=version)

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
        self._last_snapshot = self.archive.commit(
            doc=df,
            action=action.to_dict() if action is not None else None
        )
        return self.archive.checkout(version=self._last_snapshot.version)

    def last_version(self) -> int:
        """Get a identifier for the last version of a dataset.

        Returns
        -------
        int
        """
        return self._last_snapshot.version

    def metadata(self, version: Optional[int] = None) -> MetadataStore:
        """Get metadata that is associated with the referenced dataset version.
        If no version is specified the metadata collection for the latest
        version is returned.

        Raises a ValueError if the dataset version is unknown.

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
        if version is None:
            version = self.last_version()
        return self.metastore.get_store(version=version)

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
        self.archive.rollback(version=version)
        self.metastore.rollback(version=version)
        self._last_snapshot = self.archive.snapshots().last_snapshot()

    def snapshots(self) -> List[Snapshot]:
        """Get list of handles for all versions of a given dataset.

        Returns
        -------
        list of histore.archive.snapshot.Snapshot
        """
        return list(self.archive.snapshots())
