# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Generic store for objects handles."""

from abc import ABCMeta, abstractmethod
from typing import Dict, List, Optional

from openclean.engine.object.base import ObjectHandle


class ObjectStore(metaclass=ABCMeta):  # pargma: no cover
    """Interface for repositories that manage handles for objects of different
    types. Objects are stored internally using their respective serialized form.
    Object serialization and deserialization is the responsibility of an
    associated object factory.
    """
    @abstractmethod
    def delete_object(self, name: str, namespace: Optional[str] = None):
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
    def get_object(self, name: str, namespace: Optional[str] = None) -> ObjectHandle:
        """Get the deseralized object handle that is identified by the given name
        and namespace from the repository. Raises a KeyError if the referenced
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

    def get(self, name: str, namespace: Optional[str] = None) -> ObjectHandle:
        """Shortcut to get the deseralized object handle that is identified by
        the given name and namespace from the repository. Raises a KeyError if
        the referenced object does not exist.

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
        return self.get_object(name=name, namespace=namespace)

    @abstractmethod
    def insert_object(self, object: ObjectHandle):
        """Store an object in the repository. If an object with the same
        identifier (i.e., name and namespace) exists it will be replaced by the
        given object.

        Parameters
        ----------
        object: openclean.engine.object.base.ObjectHandle
            Object that is being inserted into the repository.
        """
        raise NotImplementedError()

    @abstractmethod
    def to_listing(self) -> List[Dict]:
        """Get a list of descriptor serializations for the registered objects.

        Returns
        -------
        list of dict
        """
        raise NotImplementedError()
