# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Volatile object repository that maintains all objects in main memory."""

from collections import defaultdict
from typing import Any, List, Optional, Set

from openclean.engine.store.base import ObjectHandle, ObjectRepository


class VolatileObjectHandle(ObjectHandle):
    """Handle for objects im main memory."""
    def __init__(
        self, object: Any, dtype: str, name: str, namespace: Optional[str] = None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        object: any
            Object that is stored in the registry,
        dtype: string
            Object data type identifier.
        name: string
            Unique object name.
        namespace: string, default=None
            Optional namespace identifier for the object.
        """
        super(VolatileObjectHandle, self).__init__(
            dtype=dtype,
            name=name,
            namespace=namespace
        )
        self.object = object

    def get_object(self) -> Any:
        """Get the object.

        Returns
        -------
        any
        """
        return self.object


class VolatileObjectRepository(ObjectRepository):
    """Object store that maintains all registered objects in a dictionary in
    main memory.
    """
    def __init__(self):
        """Initialize the dictionary that maintains the stored objects."""
        self.db = defaultdict(dict)

    def find_objects(
        self, dtype: Optional[str] = None, namespace: Optional[str] = None
    ) -> List[ObjectHandle]:
        """Get list of objects in the repository. The result can be filtered
        based on the data type and/or namespace. If both query parameters are
        None the full list of objects will be returned.

        Parameters
        ----------
        dtype: string, default=None
            Data type identifier. If given, only objects that match the type
            query string.
        namespace: string, default=None
            Namespace identifier. If given, only objects that match the
            namespace query string.

        Returns
        -------
        list of openclean.engine.store.base.ObjectHandle
        """
        # Get list of namespace identifier to filter on. If the user did not
        # specify a namespace all namespace identifiers are used.
        ns_query = [namespace] if namespace is not None else list(self.db.keys())
        if dtype is not None:
            result = list()
            # Filter on data type if a type query string was given by the user.
            for ns_key in ns_query:
                for obj in self.db.get(ns_key).values():
                    if obj.dtype == dtype:
                        result.append(obj)
        else:
            result = [obj for ns_key in ns_query for obj in self.db.get(ns_key).values()]
        return result

    def get_object(self, name: str, namespace: Optional[str] = None) -> Any:
        """Get the deseralized object that is identified by the given name and
        namespace from the repository. Raises a KeyError if the referenced
        object does not exist.

        Parameters
        ----------
        name: string
            Unique object name.
        namespace: string, default=None
            Optional identifier for the object namespace.

        Returns
        -------
        any

        Raises
        ------
        KeyError
        """
        # Get the object from the object handle. This will raise a KeyError if
        # the object does not exist.
        return self.db.get(namespace, {})[name].get_object()

    def insert_object(
        self, object: Any, name: str, dtype: str, namespace: Optional[str] = None
    ):
        """Store an object under the given name and the optional namespace. If
        an object with the same identifier exists it will be replaced by the
        given object.

        Parameters
        ----------
        object: any
            Object that is being inserted into the repository.
        name: string
            Unique object name.
        dtype: string
            Unique object data type identifier.
        namespace: string, default=None
            Optional identifier for the object namespace.
        """
        self.db[namespace][name] = VolatileObjectHandle(
            object=object,
            dtype=dtype,
            name=name,
            namespace=namespace
        )

    def namespaces(self) -> Set[str]:
        """Get list of all defined namespace identifier. Does not include the
        identifier (None) for the global namespace.

        Returns
        -------
        set of string
        """
        return {ns for ns in self.db if ns is not None}

    def remove_object(self, name: str, namespace: Optional[str] = None):
        """Remove the object that is identified by the given name and namespace
        from the repository. Raises a KeyError if the referenced object does
        not exist.

        Parameters
        ----------
        name: string
            Unique object name.
        namespace: string, default=None
            Optional identifier for the object namespace.

        Raises
        ------
        KeyError
        """
        # Get the object handle. Raises KeyError if the object does not exist.
        self.db.get(namespace, {})[name]
        # Update the database. We first delete the object. If the namespace is
        # empty after the object was deleted, we delete the namespace as well.
        ns = self.db[namespace]
        del ns[name]
        if len(ns) == 0:
            del self.db[namespace]

    def types(self) -> Set[str]:
        """Get list of all data type identifier for objects that are currently
        maintained by the repository.

        Returns
        -------
        set of string
        """
        return {obj.dtype for ns in self.db.values() for obj in ns.values()}
