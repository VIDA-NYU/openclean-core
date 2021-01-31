# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Default implementation for the object store. In this implementations all
objects are maintained as separate entries in a data store. this way the
default implementation is flexible towards the storage backend.
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from openclean.data.store.base import DataStore
from openclean.engine.object.base import ObjectHandle, ObjectFactory
from openclean.engine.store.base import ObjectStore


@dataclass
class StoredObject:
    """Metadata about objects in the store. Maintains the identifier for the
    descriptor and the data object together with the serialized object
    descriptor.
    """
    # Unique object name.
    name: str
    # Serialized object descriptor.
    descriptor: Dict
    # Unique object identifier. None for default objects that are not materialized.
    object_id: Optional[str] = None
    # Identifier for the entry in the data store taht mainatins the serialized
    # data part for the object. None for default objects that are not materialized.
    data_id: Optional[str] = None

    @property
    def is_default(self) -> bool:
        """Flag indicating whether the object is maintained by the data store
        or was provided as part of a set of default objects. Only objects in the
        data store will have an object and data identifier.

        Returns
        -------
        bool
        """
        return self.object_id is None or self.data_id is None

    def to_dict(self) -> Dict:
        """Default serialization of the object for the index store.

        Returns
        -------
        dict
        """
        return {'object_id': self.object_id, 'data_id': self.data_id}


class DefaultObjectStore(ObjectStore):  # pargma: no cover
    """Implementation of the object store interface. Maintains object descriptors
    and data objects as separate entries in a data store. Keeps descriptors in
    memory cache for fast access. Information about stored objects is also maintained
    as a separate object in the data store.

    Provides the option to initialize the store with a set of default objects.
    THese oblects can be overwritten by the user but they are not stored in the
    associated data store.
    """
    def __init__(
        self, identifier: str, factory: ObjectFactory, store: DataStore,
        defaults: Optional[List[ObjectHandle]] = None
    ):
        """Initialize the data store that is used to maintain serialized objects
        and the object index. The given identifier is used as the identifier for
        the object that maintains the index.

        Parameters
        ----------
        identifier: string
            Identifier of the object in the object store that maintains the
            object index.
        factory: openclean.engine.object.base.ObjectFactory
            Factory to serialize and deserialize stored objects.
        store: openclean.data.store.base.ObjectStore
            Store for serialized objects.
        defaults: dict
            Set of default object handles.
        """
        self.identifier = identifier
        self.factory = factory
        self.store = store
        self.defaults = defaultdict(dict)
        # Read the object index and the object descriptors from the store. The
        # index is a dictionary of dictionaries (namespaces) that maintain
        # metadata for the stored objects.
        try:
            doc = self.store.read_object(self.identifier)
        except KeyError:
            doc = []
        self.index = defaultdict(dict)
        # Start by adding the default objects. Make sure to add the default
        # objects to the internal defaults index.
        for obj in defaults if defaults is not None else []:
            descriptor, _ = self.factory.serialize(obj)
            name = descriptor['name']
            namespace = descriptor['namespace']
            self.index[namespace][name] = StoredObject(
                name=name,
                descriptor=descriptor
            )
            self.defaults[namespace][name] = obj
        for obj in doc:
            object_id = obj['object_id']
            descriptor = self.store.read_object(object_id)
            name = descriptor['name']
            self.index[descriptor['namespace']][name] = StoredObject(
                object_id=object_id,
                data_id=obj['data_id'],
                name=name,
                descriptor=descriptor
            )

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
        # Get the stored object information. This will raise a KeyError if the
        # object is unknown.
        object = self.index.get(namespace, {})[name]
        # Remove the seriaized object parts from the store. This is only possible
        # if the object is not a default object (which aren't maintained by the
        # data store).
        if not object.is_default:
            self.store.delete_object(object.object_id)
            self.store.delete_object(object.data_id)
        # Remove the object from the index.
        del self.index[namespace][name]
        # Write the modified index to the data store.
        self._write_index()

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
        # Get the stored object information. This will raise a KeyError if the
        # object is unknown.
        object = self.index.get(namespace, {})[name]
        # Create object from serialized descriptor and data object. If this is
        # a default object we can retrieve the handle directly from the default
        # dictionary.
        if object.is_default:
            return self.defaults[namespace][name]
        else:
            return self.factory.deserialize(
                descriptor=object.descriptor,
                data=self.store.read_object(object.data_id)
            )

    def insert_object(self, object: ObjectHandle):
        """Store an object in the repository. If an object with the same
        identifier (i.e., name and namespace) exists it will be replaced by the
        given object.

        Parameters
        ----------
        object: openclean.engine.object.base.ObjectHandle
            Object that is being inserted into the repository.
        """
        # Serialize the object descriptor and data part. Both items are stored
        # as separate objects.
        descriptor, data = self.factory.serialize(object)
        object_id = self.store.write_object(descriptor)
        data_id = self.store.write_object(data)
        # Add the object information to the index and write the modified index
        # to the data store.
        self.index[object.namespace][object.name] = StoredObject(
            object_id=object_id,
            data_id=data_id,
            name=object.name,
            descriptor=descriptor
        )
        self._write_index()
        # If the object refers to a default object that object is removed since
        # it has been overwritten by the new object.
        try:
            del self.defaults.get(object.namespace, {})[object.name]
        except KeyError:
            pass

    def to_listing(self) -> List[Dict]:
        """Get a list of descriptor serializations for the registered objects.

        Returns
        -------
        list of dict
        """
        return [obj.descriptor for ns in self.index.values() for obj in ns.values()]

    def _write_index(self):
        """Serialize the object index and write it to the data store."""
        # Make sure to only write non-default objects to the index.
        self.store.write_object(
            object=[obj.to_dict() for ns in self.index.values() for obj in ns.values() if not obj.is_default],
            object_id=self.identifier
        )
