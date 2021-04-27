# This file is part of the History Store (histore).
#
# Copyright (C) 2018-2021 New York University.
#
# The History Store (histore) is released under the Revised BSD License. See
# file LICENSE for full license details.

"""Implementation of the metadata store class that maintains metadata
information about dataset snapshots in files on the local file system.
Metadata for each snapshot is maintained in a separate directory with different
json files for each identifiable object.
"""

from typing import Callable, Dict, Optional

import json
import os
import shutil

from openclean.data.metadata.base import MetadataStore, MetadataStoreFactory
from openclean.data.serialize import default_decoder, DefaultEncoder

import histore.util as util


class FileSystemMetadataStore(MetadataStore):
    """Metadata store that maintains annotations for a dataset snapshot in JSON
    files with a given base directory. The files that maintain annotations
    are named using the FileSystemMetadataStoreFactory resource identifier.
    The following are the file names of metadata files for different types of
    resources:

    - ds.json: Dataset annotations
    - col_{column_id}.json: Column annotations
    - row_{row_id}.json: Row annotations
    - cell_{column_id}_{row_id}.json: Dataset cell annotations.
    """
    def __init__(
        self, basedir: str, encoder: Optional[json.JSONEncoder] = None,
        decoder: Optional[Callable] = None
    ):
        """Initialize the base directory and the optional JSON encoder and
        decoder.

        Parameters
        ----------
        basedir: string
            Path to the base directory for all annotation files. The directory
            is created if it does not exist.
        encoder: json.JSONEncoder, default=None
            Encoder for JSON objects.
        decoder: callable: default=None
            Object hook when decoding JSON objects.
        """
        # Create base directory if it does not exist.
        self.basedir = util.createdir(basedir)
        # Use the default decoder if None is given.
        self.decoder = decoder if decoder is not None else default_decoder
        # Use the default JSONEncoder if no encoder is given
        self.encoder = encoder if encoder is not None else DefaultEncoder

    def read(
        self, column_id: Optional[int] = None, row_id: Optional[int] = None
    ) -> Dict:
        """Read the annotation dictionary for the specified object.

        Parameters
        ----------
        column_id: int, default=None
            Column identifier for the referenced object (None for rows or full
            datasets).
        row_id: int, default=None
            Row identifier for the referenced object (None for columns or full
            datasets).

        Returns
        -------
        dict
        """
        filename = os.path.join(self.basedir, FILE(column_id, row_id))
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                return json.load(f, object_hook=self.decoder)
        return dict()

    def write(
        self, doc: Dict, column_id: Optional[int] = None,
        row_id: Optional[int] = None
    ):
        """Write the annotation dictionary for the specified object.

        Parameters
        ----------
        doc: dict
            Annotation dictionary that is being written to file.
        column_id: int, default=None
            Column identifier for the referenced object (None for rows or full
            datasets).
        row_id: int, default=None
            Row identifier for the referenced object (None for columns or full
            datasets).

        Returns
        -------
        dict
        """
        filename = os.path.join(self.basedir, FILE(column_id, row_id))
        with open(filename, 'w') as f:
            return json.dump(doc, f, cls=self.encoder)


class FileSystemMetadataStoreFactory(MetadataStoreFactory):
    """Factory pattern for volatile metadata stores."""
    def __init__(
        self, basedir: str, encoder: Optional[json.JSONEncoder] = None,
        decoder: Optional[Callable] = None
    ):
        """Initialize the base directory and the optional JSON encoder and
        decoder.

        Parameters
        ----------
        basedir: string
            Path to the base directory for all annotation files. The directory
            is created if it does not exist.
        encoder: json.JSONEncoder, default=None
            Encoder for JSON objects.
        decoder: callable: default=None
            Object hook when decoding JSON objects.
        """
        # Create base directory if it does not exist.
        self.basedir = util.createdir(basedir)
        # Use the default decoder if None is given.
        self.decoder = decoder if decoder is not None else default_decoder
        # Use the default JSONEncoder if no encoder is given
        self.encoder = encoder if encoder is not None else DefaultEncoder

    def get_store(self, version: int) -> FileSystemMetadataStore:
        """Get the metadata store for the dataset snapshot with the given version
        identifier.

        Parameters
        ----------
        version: int
            Unique version identifier

        Returns
        -------
        openclean.data.metadata.fs.FileSystemMetadataStore
        """
        metadir = os.path.join(self.basedir, str(version))
        return FileSystemMetadataStore(
            basedir=metadir,
            encoder=self.encoder,
            decoder=self.decoder
        )

    def rollback(self, version: int):
        """Remove metadata for all dataset versions that are after the given
        rollback version.

        Parameters
        ----------
        version: int
            Unique identifier of the rollback version.
        """
        metadir = os.path.join(self.basedir, str(version))
        while os.path.isdir(metadir):
            shutil.rmtree(metadir)
            version += 1
            metadir = os.path.join(self.basedir, str(version))


# -- Helper functions ---------------------------------------------------------

def FILE(column_id: Optional[int] = None, row_id: Optional[int] = None) -> str:
    """Get name for metadata file. The file name depends on whether identifier
    for the column and row are given or not. The following are the file names
    of metadata files for different types of resources:

    - ds.json: Dataset annotations
    - col_{column_id}.json: Column annotations
    - row_{row_id}.json: Row annotations
    - cell_{column_id}_{row_id}.json: Dataset cell annotations.

    Parameters
    ----------
    snapshot_id: int
        Unique snapshot version identifier.
    metadata_id: int
        Unique metadata object identifier.

    Returns
    -------
    string
    """
    if column_id is None and row_id is None:
        return 'ds.json'
    elif row_id is None:
        return 'col_{}.json'.format(column_id)
    elif column_id is None:
        return 'row_{}.json'.format(row_id)
    return 'cell_{}_{}.json'.format(column_id, row_id)
