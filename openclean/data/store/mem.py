# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""In-memory implementation for the object store interface."""

from typing import Any, Optional

from openclean.data.store.base import DataStore

import openclean.util.core as util


class VolatileDataStore(DataStore):
    """The in-memory object store maintains all objects in a dictionary in
    main memory.
    """
    def __init__(self):
        """Initialize the dictionary that maintains all stored objects."""
        self.objects = dict()

    def delete_object(self, object_id: str) -> bool:
        """Remove the object with the given identifier. Raises a KeyError if
        the object identifier is unknown.

        Parameters
        ----------
        object_id: string
            Unique object identifier.

        Raises
        ------
        KeyError
        """
        del self.objects[object_id]

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
        return self.objects[object_id]

    def write_object(self, object: Any, object_id: Optional[str] = None) -> str:
        """Store an object in the repository. Replaces an eventually existing
        object if the object identifier is given. Returns the unique object
        identifier.

        Parameters
        ----------
        object: any
            Dictionary serialization for an object.
        object_id: str, default=None
            Optional identifier for the stored object. If not given, a unique
            identifier will be generated.

        Returns
        -------
        string
        """
        # Create a new object identifier if none is given.
        object_id = object_id if object_id is not None else util.unique_identifier()
        self.objects[object_id] = object
        return object_id
