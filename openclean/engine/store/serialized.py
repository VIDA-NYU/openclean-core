# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""The serialized object repository is an object repository that persists all
objects as serialized strings in secondary storage. Objects are stored as
key-value pairs. The key is a unique object identifier that is automatically
assigned (using random UUIDs). The value is the serialized object string.

The implementation of the serialized object repository is flexible in two ways.
First, serialization and deserialization of object is done via type-specific
implementations of the :class:ObjectSerializer interface. Implementations of
serializer for different object types and serialization formats are associated
with type identifier to convert object instances to and from serializations.

A second degree of flexibility is achieved by the :class:SerializedObjectStore
interface. This interface defines the basic methods to store and retrieve the
key-value pairs representing serialized objects from different storage backends.
Implementations of this class can store objects for example as files on the
local file system, in relational databases, using Cloud storage (e.g., AWS S3
buckets), etc.. Serializations that are maintained as files on the file system
can also be further persistet in git repositories (locally or remote).

Note that the object store allows storage of serialized objects on remote servers.
This would allows for sharing of repositories between different machines and
users. The default implementation of the serialized object repository, however,
maintains a copy of the maintained object handles in memory for performance
reasons. It is therefore not intended for concurrent write operations and should
be used in a read-only fashion only.
"""

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set

import json

from histore.archive.store.fs.reader import default_decoder
from histore.archive.store.fs.writer import DefaultEncoder

from openclean.util.core import unique_identifier
from openclean.engine.store.base import ObjectHandle, ObjectRepository


# -- Object serializer --------------------------------------------------------

class ObjectSerializer(object):  # pragma: no cover
    """Interface for object serializer. The idea is to support type-specific
    object serializers to be able to store different types of objects as
    serialized string within the same object store.
    """
    @abstractmethod
    def deserialize(self, value: str) -> Any:
        """Convert an object serialization that was generated by the object
        serializer into an instance of the object type that is supported by
        this serializer.

        Parameters
        ----------
        value: string
            Serialized object string created by this serializer.

        Returns
        -------
        object
        """
        raise NotImplementedError()

    @abstractmethod
    def serialize(self, object: Any) -> str:
        """Serialize the given object.

        Parameters
        ----------
        object: any
            Object of type that is supported by the serializer.

        Returns
        -------
        string
        """
        raise NotImplementedError()


class JsonSerializer(ObjectSerializer):
    """Default implementation for the object serializer that serializes objects
    as Json string.
    """
    def __init__(
        self, encoder: Optional[json.JSONEncoder] = None, decoder: Optional[Callable] = None
    ):
        """Initialize the Json encoder and decoder hooks.

        Parameters
        ----------
        encoder: json.JSONEncoder
            Optional Json encoder for data types that are not serializable
            by default.
        decoder: callable
            Object hook for deserialization.
        """
        self.encoder = encoder if encoder is not None else DefaultEncoder
        self.decoder = decoder if decoder is not None else default_decoder

    def deserialize(self, value: str) -> Any:
        """Convert a string into a Json object.

        Parameters
        ----------
        value: string
            Serialized object string created by this serializer.

        Returns
        -------
        object
        """
        return json.loads(value, object_hook=self.decoder)

    def serialize(self, object: Any) -> str:
        """Serialize the given object.

        Parameters
        ----------
        object: any
            Object of type that is supported by the serializer.

        Returns
        -------
        string
        """
        return json.dumps(object, cls=self.encoder)


# -- Object store -------------------------------------------------------------

class SerializedObjectStore(metaclass=ABCMeta):  # pargma: no cover
    """Interface for a simple object store that maintains serialized objects
    as key-value pairs. Provides methods to access and manipulate objects in
    the store.

    Defines a `commit()` method to signal the end of operation sequences, i.e.,
    during delete and insert the object repository manipulates two entries, the
    entry for the object itself and the entry for the metadata index. Commit is
    called after the second update operation. This allows the object store to
    push changes to a remote repository (e.g., for store implementation that
    maintain all data files in a git repository).
    """
    @abstractmethod
    def commit(self):
        """Signal the end of a sequence of operations. Commit is called by the
        default object repository at the end of insert and delete operations.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_object(self, key: str):
        """Delete the object with the given key from the repository. Does not
        raise an error if no object with the given key exists.

        Parameters
        ----------
        key: string
            Unique object identifier.
        """
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()

    @abstractmethod
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
        raise NotImplementedError()


# -- Serialized object repository ---------------------------------------------

"""Metadata file name."""
METADATA_FILE = '.registry'


class SerializedObjectHandle(ObjectHandle):
    """Handle for objects that are stored as serialized strings in the default
    repository. The handle maintains the name, namespace, data type, and the
    unique object identifier. The repository handle also maintains a reference
    to the object store to allow loading the stored object.
    """
    def __init__(
        self, identifier: str, dtype: str, name: str, store: SerializedObjectStore,
        serializer: ObjectSerializer, namespace: Optional[str] = None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        identifier: string
            Unique object identifier.
        dtype: string
            Object data type identifier.
        name: string
            Unique object name.
        store: openclean.engine.store.serialized.SerializedObjectStore
            Object store that maintains object serializations.
        serializer: openclean.engine.store.serialized.ObjectSerializer
            Object serializer that is used to deserialize the object.
        namespace: string, default=None
            Optional namespace identifier for the object.
        """
        super(SerializedObjectHandle, self).__init__(
            dtype=dtype,
            name=name,
            namespace=namespace
        )
        self.identifier = identifier
        self.store = store
        self.serializer = serializer

    @staticmethod
    def from_dict(
        doc: Dict, store: SerializedObjectStore, serializers: Dict[str, ObjectSerializer]
    ) -> SerializedObjectHandle:
        """Create an object descriptor instance from a dictionary serialization.

        Parameters
        ----------
        doc: dict
            Object descriptor serialization as created by the `to_dict()` method.
        store: openclean.engine.store.serialized.SerializedObjectStore
            Object store that maintains object serializations.
        serializers: dict, default=None
            Mapping of type identifier to the respective object serializer. By
            default the Json serializer is used.

        Returns
        -------
        openclean.engine.store.serialized.SerializedObjectHandle
        """
        # Get the serializer for the given object type. Use the Json serializer
        # as the default.
        dtype = doc['dtype']
        serializer = serializers.get(dtype, JsonSerializer())
        if serializer is None:
            # Use Json serializer by default.
            serializer = JsonSerializer()
        return SerializedObjectHandle(
            identifier=doc['id'],
            name=doc['name'],
            namespace=doc['namespace'],
            dtype=dtype,
            store=store,
            serializer=serializer
        )

    def get_object(self) -> Any:
        """Get the deserialized object from the object store.

        Returns
        -------
        any
        """
        return self.serializer.deserialize(self.store.read_object(self.identifier))

    def to_dict(self) -> Dict:
        """Create a dictionary serialization for this descriptor.

        Returns
        -------
        dict
        """
        return {
            'id': self.identifier,
            'name': self.name,
            'namespace': self.namespace,
            'dtype': self.dtype
        }


class SerializedObjectRepository(ObjectRepository):
    """Default implementation of the object repository API that maintains all
    objects in a given object store. Each object is assigned a unique identifier
    and serialized using a type-specific serializer. Serialized objects are
    stored with their unique identifier as key in a key-value store.

    Maintains the list of object descriptors in a memory cache. This implementation
    is therefore not suitable for concurrent access where multiple user concurrently
    access and modify data that is stored remote.
    """
    def __init__(
        self, store: SerializedObjectStore,
        serializers: Optional[Dict[str, ObjectSerializer]] = None
    ):
        """Initialize the object store and the Json encoder and decoder. If the
        metadata file exists it is read into the memory cache.

        Parameters
        ----------
        store: openclean.engine.store.serialized.SerializedObjectStore
            Implementation of the object store that maintains object serializations
            as key-value pairs.
        serializers: dict, default=None
            Mapping of type identifier to the respective object serializer. By
            default the Json serializer is used.
        """
        self.store = store
        self.serializers = serializers if serializers is not None else dict()
        # Initialize the memory cache of object metadata. Read the metadata
        # from the registry file if it exists.
        self._metadata = defaultdict(dict)
        if self.store.exists_object(METADATA_FILE):
            for doc in json.loads(self.store.read_object(METADATA_FILE)):
                obj = SerializedObjectHandle.from_dict(
                    doc,
                    store=self.store,
                    serializers=self.serializers
                )
                self._metadata[obj.namespace][obj.name] = obj

    def find_objects(
        self, dtype: Optional[str] = None, namespace: Optional[str] = None
    ) -> List[ObjectHandle]:
        """Get list of objects in the object store. The result can be filtered
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
        # specify a namespace all namespace identifier are used.
        ns_query = [namespace] if namespace is not None else list(self._metadata.keys())
        if dtype is not None:
            result = list()
            # Filter on data type if a type query string was given by the user.
            for ns_key in ns_query:
                for obj in self._metadata.get(ns_key).values():
                    if obj.dtype == dtype:
                        result.append(obj)
        else:
            result = [obj for ns_key in ns_query for obj in self._metadata.get(ns_key).values()]
        return result

    def get_object(self, name: str, namespace: Optional[str] = None) -> Dict:
        """Get the seralization for the object that is identified by the given
        name and namespace from the object store. Raises a KeyError if the
        referenced object does not exist.

        Parameters
        ----------
        name: string
            Unique object name.
        namespace: string, default=None
            Optional identifier for the object namespace.

        Returns
        -------
        dict

        Raises
        ------
        KeyError
        """
        # Get the object descriptor and return the associated object
        # serialization. Raises KeyError if the object does not exist.
        return self._metadata.get(namespace, {})[name].get_object()

    def insert_object(
        self, object: Any, name: str, dtype: str, namespace: Optional[str] = None
    ):
        """Store an object serialization under the given name and the optional
        namespace. If an object with the same identifier exists it will be
        replaced by the given object.

        Objects are stored in separate Json files. the file names are generated
        by unique (16 character strings).

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
        # Get a unique object identifier.
        object_id = unique_identifier(16)
        while self.store.exists_object(object_id):
            object_id = unique_identifier(16)
        # Store the object serialization under the unique key.
        serializer = self.serializers.get(dtype)
        if serializer is None:
            # Use Json serializer by default.
            serializer = JsonSerializer()
        self.store.write_object(object_id, serializer.serialize(object))
        # Update the metadata cache and the stored metadata on disk.
        obj = SerializedObjectHandle(
            identifier=object_id,
            name=name,
            namespace=namespace,
            dtype=dtype,
            store=self.store,
            serializer=serializer
        )
        self._metadata[obj.namespace][obj.name] = obj
        self._write_metadata()
        self.store.commit()

    def remove_object(self, name: str, namespace: Optional[str] = None):
        """Remove the object that is identified by the given name and namespace
        from the object store. Raises a KeyError if the referenced object does
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
        # Get the object descriptor. Raises KeyError if the object does not
        # exist.
        obj = self._metadata.get(namespace, {})[name]
        # Update the metadata cache. We first delete the object descriptor. If
        # the namespace is empty after the object was deleted, we delete the
        # namespace as well.
        ns = self._metadata[namespace]
        del ns[name]
        if len(ns) == 0:
            del self._metadata[namespace]
        # Delete the object file on disk after storing the updated metadata.
        self._write_metadata()
        self.store.delete_object(obj.identifier)
        self.store.commit()

    def types(self) -> Set[str]:
        """Get list of all data type identifier for objects that are currently
        maintained by the object store.

        Returns
        -------
        set of string
        """
        return {obj.dtype for ns in self._metadata.values() for obj in ns.values()}

    def _write_metadata(self):
        """Write the current metadata index to the object store."""
        doc = [obj.to_dict() for ns in self._metadata.values() for obj in ns.values()]
        self.store.write_object(METADATA_FILE, json.dumps(doc))