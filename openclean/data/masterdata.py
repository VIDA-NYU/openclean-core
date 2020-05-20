# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Master data management operators."""

import histore

from openclean.data.downloader.registry import repositories

import openclean.config as config


def archive(repository, dataset):
    """Get the existing archive for the dataset. Raises a ValueError if the
    dataset is unknown.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    dataset: string
        Unique identifier for dataset in the repository.

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
    dataset_id = DATASET_ID(repository, dataset)
    archive = None
    for descriptor in archives.list():
        if descriptor.name() == dataset_id:
            archive = archives.get(descriptor.identifier())
            break
    if archive is None:
        raise ValueError('unknown dataset {}'.format(dataset_id))
    return archive


def diff(repository, dataset, original_version, new_version):
    """Get provenance information representing the difference between two
    snapshots for a master dataset. Raises a ValueError if the dataset is
    unknown.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    dataset: string
        Unique identifier for dataset in the repository.
    original_version: int
        Unique identifier for the original version.
    new_version: int
        Unique identifier for the version that the original version is
        compared against.

    Returns
    -------
    histore.archive.provenance.archive.SnapshotDiff

    Raises
    ------
    ValueError
    """
    # Get snapshot listing from the existing archive for the dataset. Raises a
    # ValueError if the dataset is unknown.
    return archive(repository, dataset).diff(
        original_version=original_version,
        new_version=new_version
    )


def download(repository, datasets=None, properties=None, replace=False):
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
    replace: boolean, default=False
        Replace an existing archive. All changes to that archive will be lost.

    Raises
    ------
    ValueError
    """
    # Get the master data manager. Create an index of archives where archive
    # archive names are mapped to archive descriptors.
    archives = manager()
    arch_index = dict()
    for archive in archives.list():
        arch_index[archive.name()] = archive
    # Get the repository handle from the registered repositories. Raise an
    # error if an unknown repository is specified.
    rh = repositories(identifier=repository)
    if rh is None:
        raise ValueError('unknown repository {}'.format(repository))
    # Update list of datasets to avoid downloadind existing datasets that are
    # not overwritten.
    if not replace:
        if datasets is None:
            datasets = [d.identifier() for d in rh.datasets()]
        elif not isinstance(datasets, list):
            datasets = [datasets]
        none_existing_datasets = list()
        for ds in datasets:
            ds_name = DATASET_ID(repository, ds)
            if ds_name not in arch_index:
                print('{} not in {}'.format(ds_name, arch_index))
                none_existing_datasets.append(ds)
        # Finish downloaded in dataset list is empty
        if not none_existing_datasets:
            return
        datasets = none_existing_datasets
    # Get the loader class instance
    downloads = rh.download(datasets=datasets, properties=properties)
    for ds, df in downloads:
        ds_name = DATASET_ID(repository, ds.identifier())
        if ds_name not in arch_index:
            descriptor = archives.create(
                name=ds_name,
                primary_key=ds.primary_key()
            )
            arch_index[ds_name] = descriptor
        elif replace:
            # Delete an existing archive.
            identifier = arch_index[ds_name].identifier()
            archives.delete(identifier)
            descriptor = archives.create(
                name=ds_name,
                primary_key=ds.primary_key()
            )
            arch_index[ds_name] = descriptor
        archives.get(arch_index[ds_name].identifier()).commit(df)


def load(repository, dataset, version=None):
    """Get data frame for downloaded master dataset. Raises a ValueError if
    the dataset is unknown.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    dataset: string
        Unique identifier for dataset in the repository.
    version: int, default=None
        Version if the dataset that is being retrieved from the archive.
        Returns the latest version by default if the value is not set.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    # Checkout a given snapshot version from the existing archive for the
    # dataset. Raises a ValueError if the dataset is unknown.
    return archive(repository, dataset).checkout(version=version)


def manager():
    """Get instance of the archive manager that is used to maintain downloaded
    master datasets.

    Returns
    -------
    histore.archive.manager.base.ArchiveManager
    """
    return histore.PersistentArchiveManager(basedir=config.MASTERDATADIR())


def snapshots(repository, dataset):
    """Get list of snapshots for a master dataset. Raises a ValueError if the
    dataset is unknown.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    dataset: string
        Unique identifier for dataset in the repository.

    Returns
    -------
    histore.archive.snapshot.SnapshotListing

    Raises
    ------
    ValueError
    """
    # Get snapshot listing from the existing archive for the dataset. Raises a
    # ValueError if the dataset is unknown.
    return archive(repository, dataset).snapshots()


def update(
    repository, dataset, df, description=None, renamed=None, renamed_to=True,
    partial=False, origin=None
):
    """Update a previously downloaded dataset with a new given snapshot. Raises
    a ValueError if the dataset is unknown.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    dataset: string
        Unique identifier for dataset in the repository.
    df: pandas.DataFrame
        New dataset snapshot.
    description: string, default=None
        Optional user-provided description for the snapshot.
    renamed: dict, default=None
        Optional mapping of columns that have been renamed. Maps the new
        column name to the original name.
    renamed_to: bool, default=True
        Flag that determines the semantics of the mapping in the renamed
        dictionary. By default a mapping from the original column name
        (i.e., the dictionary key) to the new column name (the dictionary
        value) is assumed. If the flag is False a mapping from the new
        column name to the original column name is assumed.
    partial: bool, default=False
        If True the given snapshot is assumed to be partial. All columns
        from the snapshot schema that is specified by origin that are not
        matched by any column in the snapshot schema are assumed to be
        unchanged. All rows from the orignal snapshot that are not in the
        given snapshot are also assumed to be unchnged.
    origin: int, default=None
        Version identifier of the original column against which the given
        column list is matched.

    Raises
    ------
    ValueError
    """
    # Merge the given snapshot into the existing archive for the dataset.
    # Raises a ValueError if the dataset is unknown.
    archive(repository, dataset).commit(
        doc=df,
        description=description,
        renamed=renamed,
        renamed_to=renamed_to,
        partial=partial,
        origin=origin
    )


# -- Helper methods -----------------------------------------------------------

def DATASET_ID(repository, dataset):
    """Get global unique dataset identifier as a combination of the repository
    identifier and the repository-specific dataset identifier.

    Parameters
    ----------
    repository: string
        Unique master data repository identifier.
    dataset: string
        Unique identifier for dataset in the repository.

    Returns
    -------
    string
    """
    return '{}/{}'.format(repository, dataset)
