# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""The openclean engine maintains a collection of datasets. Each dataset is
identified by a unique name. Dataset snapshots are maintained by a datastore.

The idea of the engine is to provide a namespace for datasets that are maintained
by a datastore which keeps track of changes to the data. The engine is associated
with an object registry that maintains user-defined objects such as functions,
lookup tables, etc..
"""

from typing import List, Optional, Tuple, Union

import pandas as pd
import os

from openclean.data.archive.base import ArchiveManager, Datasource, Descriptor, Snapshot
from openclean.data.archive.base import PersistentArchiveManager, VolatileArchiveManager
from openclean.data.archive.cache import CachedDatastore
from openclean.data.archive.histore import HISTOREDatastore
from openclean.data.metadata.base import MetadataStore
from openclean.data.metadata.fs import FileSystemMetadataStoreFactory
from openclean.data.metadata.mem import VolatileMetadataStoreFactory
from openclean.data.serialize import COMPACT
from openclean.engine.action import LoadOp, OpHandle
from openclean.engine.dataset import DatasetHandle, DataSample, FullDataset, StreamOpPipeline
from openclean.engine.library import ObjectLibrary
from openclean.engine.registry import registry
from openclean.pipeline import DataPipeline

import openclean.util.core as util


class OpencleanEngine(object):
    """The idea of the engine is to provide a namespace that manages a set of
    datasets that are identified by unique names. The engine is associated with
    an object repository that provides additional functionality to register
    objects like functions, lookup tables, etc..

    Datasets that are created from files of data frames are maintained by an
    archive manager.

    Each engine has a unique identifier allowing a user to use multiple
    engines if necessary.
    """
    def __init__(
        self, identifier: str, manager: ArchiveManager, library: ObjectLibrary,
        basedir: Optional[str] = None, cached: Optional[bool] = True
    ):
        """Initialize the engine identifier and the manager for created dataset
        archives.

        Parameters
        ----------
        identifier: string
            Unique identifier for the engine instance.
        manager: histore.archive.manager.base.ArchiveManager
            Manager for created dataset archives.
        library: openclean.engine.object.base.ObjectLibrary
            Library manager for objects (e.g., registered functions).
        basedir: string, default=None
            Path to directory on disk where archive metadata is maintained.
        cached: bool, default=True
            Flag indicating whether the all datastores that are created for
            existing archives are cached datastores or not.
        """
        self.identifier = identifier
        self.manager = manager
        self.basedir = basedir
        # Library of objects that are available to the user of the engine.
        self.library = library
        # Dictionary for the maintained datasets. Maintains data engines that
        # contain the archive identifier and references to the datastore and
        # the archive manager.
        # The identifier and manager are only set for persistent datasets to
        # allow dropping them.
        self._datasets = dict()
        # Initialize all archives that are maintained by the manager.
        for descriptor in self.manager.list():
            archive_id = descriptor.identifier()
            archive = self.manager.get(archive_id)
            if self.basedir is not None:
                metadir = os.path.join(self.basedir, archive_id)
                metastore = FileSystemMetadataStoreFactory(basedir=metadir)
            else:
                metastore = VolatileMetadataStoreFactory()
            datastore = HISTOREDatastore(archive=archive, metastore=metastore)
            if cached:
                # Wrapped datastore into a cached store if requested.
                datastore = CachedDatastore(datastore=datastore)
            self._datasets[descriptor.name()] = FullDataset(
                datastore=datastore,
                manager=self.manager,
                identifier=archive_id,
                pk=descriptor.primary_key()
            )

    def apply(
        self, name: str, operations: StreamOpPipeline, validate: Optional[bool] = None
    ) -> List[Snapshot]:
        """Apply a given operator or a sequence of operators on the specified
        archive.

        The resulting snapshot(s) will directly be merged into the archive. This
        method allows to update data in an archive directly without the need
        to checkout the snapshot first and then commit the modified version(s).

        Returns list of handles for the created snapshots.

        Note that there are some limitations for this method. Most importantly,
        the order of rows cannot be modified and neither can it insert new rows
        at this point. Columns can be added, moved, renamed, and deleted.

        Parameters
        ----------
        name: string
            Unique dataset name.
        operations: openclean.engine.dataset.StreamOpPipeline
            Operator(s) that is/are used to update the rows in a dataset
            snapshot to create new snapshot(s) in this archive.
        validate: bool, default=False
            Validate that the resulting archive is in proper order before
            committing the action.

        Returns
        -------
        histore.archive.snapshot.Snapshot
        """
        return self.dataset(name).apply(operations=operations, validate=validate)

    def checkout(self, name: str, commit: Optional[bool] = False) -> pd.DataFrame:
        """Checkout the latest version of a dataset. The dataset is identified
        by the unique name. If the dataset that is currently associated with
        the given name is a sample dataset it will be replace by the handle for
        the original dataset first. If the commit flag is True any uncommited
        changes for the sample dataset will be commited first.

        Raises a KeyError if the given dataset name is unknown.

        Parameters
        ----------
        name: string
            Unique dataset name.
        commit: bool, default=False
            Apply all uncommited changes to the original database if True.

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        KeyError
        """
        dataset = self.dataset(name)
        if dataset.is_sample:
            if commit:
                # Only need to commit anything if the dataset is a sample.
                dataset.apply()
            # Replace the sampled dataset with its original.
            self._datasets[name] = dataset.original
        return self.dataset(name=name).checkout()

    def commit(
        self, name: str, source: Datasource, action: Optional[OpHandle] = None
    ) -> Datasource:
        """Commit a modified data frame to the dataset archive.

        The dataset is identified by its unique name. Raises a KeyError if the
        given dataset name is unknown.

        Parameters
        ----------
        name: string
            Unique dataset name.
        source: openclean.data.stream.base.Datasource
            Input data frame or stream containing the new dataset version that
            is being stored.
        action: openclean.engine.action.OpHandle, default=None
            Operator that created the dataset snapshot.

        Returns
        -------
        openclean.data.stream.base.Datasource

        Raises
        ------
        KeyError
        """
        return self.dataset(name=name).commit(source=source, action=action)

    def create(
        self, source: Datasource, name: str,
        primary_key: Optional[Union[List[str], str]] = None,
        cached: Optional[bool] = True
    ) -> DatasetHandle:
        """Create an initial dataset archive that is idetified by the given
        name. The given data represents the first snapshot in the created
        archive.

        Raises a ValueError if an archive with the given name already exists.

        Parameters
        ----------
        source: pd.DataFrame, CSVFile, or string
            Data frame or file containing the first version of the archived
            dataset.
        name: string
            Unique dataset name.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for rows in the
            archive.
        cached: bool, default=True
            Flag indicating whether the last accessed dataset snapshot for
            the created dataset is cached for fast access.

        Returns
        -------
        openclean.engine.dataset.DatasetHandle

        Raises
        ------
        ValueError
        """
        # Ensure that the dataset name is unique.
        if name in self._datasets:
            raise ValueError("dataset '{}' exists".format(name))
        # Create a new dataset archive with the associated manager.
        descriptor = self.manager.create(
            name=name,
            doc=source,
            primary_key=primary_key,
            serializer=COMPACT,
            snapshot=Descriptor(action=LoadOp().to_dict())
        )
        archive_id = descriptor.identifier()
        archive = self.manager.get(archive_id)
        # Commit the given dataset to the archive. TODO: We should add a LoadOp
        # class to represent the action.
        # Create a datastore to manage the archive and register that datastore
        # with this engine under the given name.
        if self.basedir is not None:
            metadir = os.path.join(self.basedir, archive_id)
            metastore = FileSystemMetadataStoreFactory(basedir=metadir)
        else:
            metastore = VolatileMetadataStoreFactory()
        datastore = HISTOREDatastore(archive=archive, metastore=metastore)
        if cached:
            # Wrapped datastore into a cached store if requested.
            datastore = CachedDatastore(datastore=datastore)
        self._datasets[name] = FullDataset(
            datastore=datastore,
            manager=self.manager,
            identifier=archive_id,
            pk=primary_key
        )
        # Return handle for the created dataset.
        return self._datasets[name]

    def dataset(self, name: str) -> DatasetHandle:
        """Get handle for a dataset. Depending on the type of the dataset this
        will either return a :class:FullDataset or :class:DataSample.

        Parameters
        ----------
        name: string
            Unique dataset name.

        Returns
        -------
        openclean.engine.dataset.DatasetHandle
        """
        if name not in self._datasets:
            raise ValueError("unknown dataset '{}'".format(name))
        return self._datasets[name]

    def drop(self, name: str):
        """Delete the full history for the dataset with the given name. Raises
        a ValueError if the dataset name is unknonw.

        Parameters
        ----------
        name: string
            Unique dataset name.

        Raises
        ------
        ValueError
        """
        self.dataset(name).drop()
        del self._datasets[name]

    def load_dataset(
        self, source: Datasource, name: str,
        primary_key: Optional[Union[List[str], str]] = None,
        cached: Optional[bool] = True
    ) -> DatasetHandle:
        """Create an initial dataset archive that is idetified by the given
        name. The given data frame represents the first snapshot in the created
        archive.

        Raises a ValueError if an archive with the given name already exists.

        This is a synonym for create() for backward compatibility.

        Parameters
        ----------
        source: pd.DataFrame or string
            Data frame or file containing the first version of the archived
            dataset.
        name: string
            Unique dataset name.
        primary_key: string or list, default=None
            Column(s) that are used to generate identifier for rows in the
            archive.
        cached: bool, default=True
            Flag indicating whether the last accessed dataset snapshot for
            the created dataset is cached for fast access.

        Returns
        -------
        openclean.engine.dataset.DatasetHandle

        Raises
        ------
        ValueError
        """
        return self.create(
            source=source,
            name=name,
            primary_key=primary_key,
            cached=cached
        )

    def metadata(self, name: str) -> MetadataStore:
        """Get metadata that is associated with the current dataset version.

        Raises a ValueError if the dataset is unknown.

        Parameters
        ----------
        name: string
            Unique dataset name.

        Returns
        -------
        openclean.data.metadata.base.MetadataStore
        """
        return self.dataset(name=name).metadata()

    @property
    def register(self) -> ObjectLibrary:
        """Synonym for accessing the library as a function registry.

        Returns
        -------
        openclean.engine.object.base.ObjectLibrary
        """
        return self.library

    def rollback(self, name: str, version: str) -> pd.DataFrame:
        """Rollback all changes including the given dataset version.

        That is, we rollback all changes that occurred at and after the
        identified snapshot. This will make the respective snapshot of the
        previous version the new current (head) snapshot for the dataset
        history.

        Returns the dataframe for the dataset snapshot that is at the new head
        of the dataset history.

        Raises a KeyError if the dataset or the given version identifier are
        unknown.

        Parameters
        ----------
        name: string
            Unique dataset name.
        version: string
            Unique log entry version.

        Returns
        -------
        pd.DataFrame
        """
        return self.dataset(name=name).rollback(version=version)

    def sample(
        self, name: str, n: Optional[int] = None,
        random_state: Optional[Tuple[int, List]] = None
    ) -> pd.DataFrame:
        """Display the spreadsheet view for a given dataset. The dataset is
        identified by its unique name. Raises a ValueError if no dataset with
        the given name exists.

        Creates a new data frame that contains a random sample of the rows in
        the last snapshot of the identified dataset. This sample is registered
        as a separate dataset with the engine. If neither n nor frac are
        specified a random sample of size 100 is generated.

        Parameters
        ----------
        name: string
            Unique dataset name.
        n: int, default=None
            Number of rows in the sample dataset.
        random_state: int or list, default=None
            Seed for random number generator.

        Returns
        -------
        pd.DataFrame

        Raises
        ------
        KeyError
        """
        # Create a sample of size 100 if neither n nor frac is given.
        n = 100 if n is None else n
        # Get the handle for the referenced dataset and checkout the latest
        # dataset snapshot.
        handle = self.dataset(name)
        # Create a random sample from the dataset.
        df = DataPipeline(source=handle.open())\
            .sample(n=n, random_state=random_state)\
            .to_df()
        # Register the generated sample as a new dataset with a reference to
        # the original dataset to maintain the link to the source of a sampled
        # dataset.
        ds = DataSample(df=df, original=handle, n=n, random_state=random_state)
        self._datasets[name] = ds
        return df

    def stream(self, name: str, version: Optional[int] = None) -> DataPipeline:
        """Get a data pipeline for a dataset snapshot.

        Parameters
        ----------
        name: string
            Unique dataset name.
        version: int, default=None
                Unique version identifier. By default the last version is used.

        Returns
        -------
        openclean.pipeline.DataPipeline
        """
        handle = self.dataset(name)
        # Create a random sample from the dataset.
        return DataPipeline(source=handle.open(version=version))


# -- Engine factory -----------------------------------------------------------

def DB(
    basedir: Optional[str] = None, create: Optional[bool] = False,
    cached: Optional[bool] = True
) -> OpencleanEngine:
    """Create an instance of the openclean engine. Uses a persistent engine if
    a base directory is given. This test implementation uses HISTORE as the
    underlying datastore for a persistent engine. If no base directory is given,
    a volatile archive manager will be used instead of a persistent one.

    If the create flag is True all existing files in the base directory (if
    given) will be removed.

    Parameters
    ----------
    basedir: string
        Path to directory on disk where archives are maintained.
    create: bool, default=False
        Create a fresh instance of the archive manager if True. This will
        delete all files in the base directory.
    cached: bool, default=True
        Flag indicating whether the all datastores that are created for
        existing archives are cached datastores or not.

    Returns
    -------
    openclean.engine.base.OpencleanEngine
    """
    # Create a unique identifier to register the created engine in the
    # global registry dictionary. Use an 8-character key here. Make sure to
    # account for possible conflicts.
    engine_id = util.unique_identifier(8)
    while engine_id in registry:
        engine_id = util.unique_identifier(8)
    # Create the engine components and the engine instance itself.
    if basedir is not None:
        histore = PersistentArchiveManager(basedir=basedir, create=create)
        metadir = os.path.join(basedir, '.metadata')
    else:
        histore = VolatileArchiveManager()
        metadir = None
    engine = OpencleanEngine(
        identifier=engine_id,
        manager=histore,
        library=ObjectLibrary(),
        basedir=metadir,
        cached=cached
    )
    # Register the new engine instance before returning it.
    registry[engine_id] = engine
    return engine
