# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Implementation of the object store that maintains all object serializations
as files on the local disk.
"""

from __future__ import annotations

import os

from openclean.engine.store.serialized import SerializedObjectStore


"""Metadata file name."""
METADATA_FILE = '.registry'


class FileSystemObjectStore(SerializedObjectStore):
    """Object store that maintains all key-value pairs as files on disk.
    The object key is used as the file name.
    """
    def __init__(self, basedir: str):
        """Initialize the base directory where all object files are stored.
        If the base directory does not exist it is created.

        Parameters
        ----------
        basedir: string
            Base directory for object files.
        """
        self.basedir = basedir
        # Ensure that the base directory exists.
        os.makedirs(self.basedir, exist_ok=True)

    def commit(self):
        """Signal the end of a sequence of operations. Nothing needs to be
        commited here.
        """
        pass

    def delete_object(self, key: str):
        """Delete the file for the object with the given key. Does not raise an
        error if no object with the given key exists.

        Parameters
        ----------
        key: string
            Unique object identifier.
        """
        filename = self.objectfile(key)
        if os.path.isfile(filename):
            os.remove(filename)

    def exists_object(self, key: str) -> bool:
        """Check if an object with the given key exists in the object store.

        Parameters
        ----------
        key: string
            Unique object identifier.

        Returns
        -------
        bool
        """
        return os.path.isfile(self.objectfile(key))

    def objectfile(self, key: str) -> str:
        """Get filepath for object with the given identifier.

        Parameters
        ----------
        key: string
            Unique object identifier.

        Returns
        -------
        string
        """
        return os.path.join(self.basedir, '{}.json'.format(key))

    def read_object(self, key: str) -> str:
        """Read the value that is associated with the given key. Returns None if
        no object with the given key exists.

        Parameters
        ----------
        key: string
            Unique object identifier.

        Returns
        -------
        string
        """
        filename = self.objectfile(key)
        if os.path.isfile(filename):
            with open(filename, 'r') as f:
                return f.read()

    def write_object(self, key: str, value: str):
        """Write key-value pair to the store. If an entry with the given key
        exists it is replaced by the given value.
        Parameters
        ----------
        key: string
            Unique object identifier.
        value: string
            Object value.
        """
        filename = self.objectfile(key)
        with open(filename, 'w') as f:
            f.write(value)
