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

from typing import List, Optional, Union

import pandas as pd

from openclean.data.archive.base import (
    Archive, ActionHandle, ArchiveSchema, ArchiveStore, DatasetOperator,
    Descriptor, Snapshot, SnapshotReader
)
from openclean.data.metadata.base import MetadataStore, MetadataStoreFactory
from openclean.data.metadata.mem import VolatileMetadataStoreFactory
from openclean.data.stream.base import Datasource


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
        snapshots = self.archive.apply(operators=operators, origin=origin, validate=validate)
        # Make sure to update the reference to the last snapshot.
        if snapshots:
            self._last_snapshot = snapshots[-1]
        return snapshots

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

    def commit(
        self, source: Datasource, action: Optional[ActionHandle] = None,
        checkout: Optional[bool] = False
    ) -> Datasource:
        """Insert a new version for a dataset.

        Returns the inserted data frame. If the ``checkout`` flag is True the
        commited dataset is checked out to account for possible changes to the
        row index. If the flag is set to False the given data frame is returned.

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
        self._last_snapshot = self.archive.commit(
            doc=source,
            descriptor=Descriptor(action=action.to_dict() if action is not None else None)
        )
        return self.archive.checkout(version=self._last_snapshot.version) if checkout else source

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
        return self.archive.open(version=version)

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

    def schema(self) -> ArchiveSchema:
        """Get the schema history for the archived dataset.

        Returns
        -------
        openclean.data.archive.base.ArchiveSchema
        """
        return self.archive.schema()

    def snapshots(self) -> List[Snapshot]:
        """Get list of handles for all versions of a given dataset.

        Returns
        -------
        list of histore.archive.snapshot.Snapshot
        """
        return list(self.archive.snapshots())
