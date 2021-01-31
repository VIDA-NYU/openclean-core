# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Base classes for data objects that are maintained by the object library.

The object handle is the base object that maintains metadata about objects in
the library. Different object types (e.g., functions, lookup table, controlled
vocabularies) implement specific object handles that extend the base class.

FOr each object type an object factory has to be implemented. The factory is
responsible for serializing and deserializing object of the specific type
for storage in the object store that is associated with the object library.
"""

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass
class ObjectHandle(object):
    """Handle for objects that are stored in the object library. For each object
    basic metadata is maintained. Each object has a user-provided name. Objects
    are grouped into user-defined namespaces. The combination of name and
    namespace forms a unique identifier for objects of the same type.

    In addition, the object descriptor contains a human-readable label and a
    descriptive help text.

    Type-specific sub-classes for the object handle may extend the basic handle
    with additional metadata. It is the responsibility of the respective factory
    that is associated with those types to correctly serialize and deseralize
    the handle.
    """
    # Object name that is unique within the namespace.
    name: str
    # Optional namespace.
    namespace: Optional[str] = None
    # Optional human-readable name for display purposes (e.g., in object listings).
    label: Optional[str] = None
    # Descriptive text  for display purposes (e.g., tooltips in a front-end).
    description: Optional[str] = None

    def to_dict(self) -> Dict:
        """Get serialization for the basic metadata that is maintained for each
        object.

        Returns
        -------
        dict
        """
        doc = {'name': self.name, 'namespace': self.namespace}
        if self.label is not None:
            doc['label'] = self.label
        if self.description is not None:
            doc['description'] = self.description
        return doc


class ObjectFactory(metaclass=ABCMeta):  # pargma: no cover
    """Factory pattern for different types of objects that are maintained by the
    object library. Each factory is responsible for creating object instances
    from object serializations and for serializing objects so that they can be
    stored in the object store that is associated with the object library.

    All objects are serialized into two parts: (i) the object descriptor that
    contains metadata which is included in object listings (e.g., for front-end
    user interfaces), and (ii) the object data containing the implementation-specific
    part of the different object types.
    """
    @abstractmethod
    def deserialize(self, descriptor: Dict, data: Any) -> ObjectHandle:
        """Create an object instance from a dictionary serialization.

        Parameters
        ----------
        descriptor: dict
            Dictionary serialization for the object descriptor as created by the
            serialize method.
        data: any
            Serialization for the implementation-specific part of the object
            handle as created by the serialize method.

        Returns
        -------
        openclean.engine.object.base.ObjectHandle
        """
        raise NotImplementedError()

    @abstractmethod
    def serialize(self, object: ObjectHandle) -> Tuple[Dict, Any]:
        """Create dictionary serialization for an object. Each object is
        serialized into two parts: (i) the object descriptor and the object
        data. The descriptor contains the metadata for an object while the
        data contains the implementation-specific part of the object.

        Parameters
        ----------
        object: openclean.engine.object.base.ObjectHandle
            Object of factory-specific type that is being serialized.

        Returns
        -------
        tuple of dict and any
        """
        raise NotImplementedError()
