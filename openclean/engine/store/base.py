# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Generic store for serialized objects (dictionaries)."""

from abc import ABCMeta, abstractmethod
from typing import Any, List, Optional, Set


class ObjectHandle(metaclass=ABCMeta):
    """Handle for stored objects. Provides access to the serialized object, the
    object name and data type, as well as the optional namespace identifier.
    """
    def __init__(self, dtype: str, name: str, namespace: Optional[str] = None):
        """Initialize the object properties.

        Parameters
        ----------
        dtype: string
            Object data type identifier.
        name: string
            Unique object name.
        namespace: string, default=None
            Optional namespace identifier for the object.
        """
        self.dtype = dtype
        self.name = name
        self.namespace = namespace

    @abstractmethod
    def get_object(self) -> Any:
        """Get the deserialized object from the object store.

        Returns
        -------
        any
        """
        raise NotImplementedError()  # pragma: no cover


class ObjectRepository(metaclass=ABCMeta):  # pargma: no cover
    """Abstract object repository class. Defines the interface for repositories
    that maintain serialized objects of different types. Objects are represented
    internally as dictionary serializations.

    Each object has a name and a data type. Objects are maintained within
    namespaces. There are no constraints on the list of valid identifier for
    data types and namespaces. These identifiers are defined by the user.
    Implementations of this abstract class may define their own constraints
    on valid identifier (e.g., maximum length in number of characters).

    The object name has to be unique within the namespace. By default, objects
    are stored in the global namespace (`namespace=None`).

    There are also no constraints on the contents of the dictionaries that
    contain object. However, depending on the implementation, there will be
    restrictions on the object that can be stored (e.g., Json serialized).
    """
    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def namespaces(self) -> Set[str]:
        """Get list of all defined namespace identifier. Does not include the
        identifier (None) for the global namespace.

        Returns
        -------
        set of string
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
    def types(self) -> Set[str]:
        """Get list of all data type identifier for objects that are currently
        maintained by the repository.

        Returns
        -------
        set of string
        """
        raise NotImplementedError()
