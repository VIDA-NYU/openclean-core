# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Implementation for the object store interface that maintains all objects as
serialized Json documents in files on the file system.
"""

from typing import Any, Callable, Optional

import json
import os

from openclean.data.serialize import default_decoder, DefaultEncoder
from openclean.data.store.base import DataStore

import openclean.util.core as util


class FileSystemJsonStore(DataStore):
    """Data store that maintains all objects as Json files in a directory on
    the file system.
    """
    def __init__(
        self, basedir: str, encoder: Optional[json.JSONEncoder] = None,
        decoder: Optional[Callable] = None
    ):
        """Initialize directory where files are stored and the optional Json
        encoder and decoder hooks. If the directory does not exist it is
        created.

        Parameters
        ----------
        basedir: string
            Directory for all created object files.
        encoder: json.JSONEncoder
            Optional Json encoder for data types that are not serializable
            by default.
        decoder: callable
            Object hook for deserialization.
        """
        self.basedir = basedir
        # Create the directory if it does not exist.
        os.makedirs(basedir, exist_ok=True)
        # Use default encoder and decoder from HISTORE.
        self.encoder = encoder if encoder is not None else DefaultEncoder
        self.decoder = decoder if decoder is not None else default_decoder

    def delete_object(self, object_id: str) -> bool:
        """Remove the file for the object with the given identifier. Raises a
        KeyError if the object identifier is unknown, i.e., if the object file
        does not exist.

        Parameters
        ----------
        object_id: string
            Unique object identifier.

        Raises
        ------
        KeyError
        """
        os.unlink(self._object_file(object_id, exist=True))

    def _object_file(self, object_id, exist=True) -> str:
        """Helper method to get the file name for the object with the given
        identifier. Raises a KeyError if the exists is True and the file does
        not exist.

        Parameters
        ----------
        object_id: string
            Unique object identifier.
        exist: bool, default=True
            Raise a KeyError if the file does not exist.

        Returns
        -------
        string

        Raises
        ------
        KeyError
        """
        filename = os.path.join(self.basedir, '{}.json'.format(object_id))
        if exist and not os.path.isfile(filename):
            raise KeyError("unknown object '{}'".format(object_id))
        return filename

    def read_object(self, object_id: str) -> Any:
        """Get the serialized object that is identified by the given object id.
        Raises a KeyError if the referenced object does not exist.

        Parameters
        ----------
        object_id: string
            Unique object identifier.

        Returns
        -------
        any

        Raises
        ------
        KeyError
        """
        with open(self._object_file(object_id, exist=True), 'r') as f:
            return json.load(f, object_hook=self.decoder)

    def write_object(self, object: Any, object_id: Optional[str] = None) -> str:
        """Store an object in the repository. If the object identifier is given
        an existing file for that identifier will be overwritten. Returns the
        unique object identifier.

        Parameters
        ----------
        object: any
            Serialization for an object.
        object_id: str, default=None
            Optional identifier for the stored object. If not given, a unique
            identifier will be generated.

        Returns
        -------
        string
        """
        # Create a new object identifier if none is given.
        object_id = object_id if object_id is not None else util.unique_identifier()
        # Write object serialization to file.
        with open(self._object_file(object_id, exist=False), 'w') as f:
            json.dump(object, f, cls=self.encoder)
        # Return the object identifier.
        return object_id
