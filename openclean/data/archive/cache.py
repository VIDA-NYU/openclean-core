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
from typing import List, Optional, Union

import pandas as pd

from openclean.data.archive.base import (
    ActionHandle, ArchiveStore, ArchiveSchema, DatasetOperator, Snapshot,
    SnapshotReader
)
from openclean.data.metadata.base import MetadataStore
from openclean.data.stream.base import Datasource
from openclean.data.stream.df import DataFrameStream


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
        datastore: openclean.data.archive.base.ArchiveStore
            Reference to the datastore that persists the datasets.
        """
        self.datastore = datastore
        self._cache = None

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
        # Invalidate the cache.
        self._cache = None
        # Apply operations to the wrapped datastore archive.
        return self.datastore.apply(operators=operators, origin=origin, validate=validate)

    def checkout(
        self, version: Optional[int] = None, no_cache: Optional[bool] = False
    ) -> pd.DataFrame:
        """Get a specific version of a dataset. The dataset snapshot is
        identified by the unique version identifier.

        Raises a ValueError if the given version is unknown.

        Parameters
        ----------
        version: int
            Unique dataset version identifier.
        no_cache: bool, default=None
            If True, ignore cached dataset version and checkout the dataset
            from the associated data store.

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
        if self._cache is not None and not no_cache:
            # If the requested version matches the cached version return the
            # cached data frame.
            if version == self._cache.version:
                return self._cache.df
        # Dataset has not been caches. Checkout the dataset from the datastore
        # and update the cache.
        df = self.datastore.checkout(version=version)
        self._cache = CacheEntry(df=df, version=version)
        return df

    def commit(
        self, source: Datasource, action: Optional[ActionHandle] = None,
        checkout: Optional[bool] = False
    ) -> Datasource:
        """Insert a new version for a dataset.

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
        source = self.datastore.commit(source=source, action=action, checkout=checkout)
        if isinstance(source, pd.DataFrame):
            self._cache = CacheEntry(df=source, version=self.datastore.last_version())
        return source

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
        # Get the latest dataset version if the argument is None.
        if version is None:
            version = self.datastore.last_version()
        # If the requested version matches the cached version return a
        # stream for the cached data frame.
        if self._cache is not None:
            if version == self._cache.version:
                return DataFrameStream(self._cache.df)
        return self.datastore.open(version=version)

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

    def schema(self) -> ArchiveSchema:
        """Get the schema history for the archived dataset.

        Returns
        -------
        openclean.data.archive.base.ArchiveSchema
        """
        return self.datastore.schema()

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
