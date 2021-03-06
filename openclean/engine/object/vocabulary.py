# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Handle and factory implementations for controlled vocabularies that are
registered with the object library.
"""

from typing import Dict, List, Optional, Tuple, Set

from openclean.engine.object.base import ObjectHandle, ObjectFactory


class VocabularyHandle(ObjectHandle):
    """Handle for controlled vocabularies that are registered with the object
    library. Extends the base handle with a set of values that form the terms
    in the vocabulary.
    """
    def __init__(
        self, values: Set, name: str, namespace: Optional[str] = None,
        label: Optional[str] = None, description: Optional[str] = None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        values: set
            Terms in the controlled vocabulary.
        name: string
            Name for the vocabulary.
        namespace: string, default=None
            Name of the namespace that this vocabulary belongs to. By default
            all vocabularies will be placed in a global namespace (None).
        label: string, default=None
            Optional human-readable name for display purposes.
        description: str, default=None
            Descriptive text for the vocabulary. This text can for example be
            displayed as a tooltip in a user interface.
        """
        super(VocabularyHandle, self).__init__(
            name=name,
            namespace=namespace,
            label=label,
            description=description
        )
        self.values = values


class VocabularyFactory(ObjectFactory):
    """Factory for controlled vocabularies."""
    def deserialize(self, descriptor: Dict, data: List) -> ObjectHandle:
        """Convert an object serialization that was generated by the object
        serializer into a vocabulary handle.

        Parameters
        ----------
        descriptor: dict
            Dictionary serialization for the vocabulary descriptor as created
            by the serialize method.
        data: list of dict
            Serialization for the terms in the vocabulary.

        Returns
        -------
        openclean.engine.object.vocabulary.VocabularyHandle
        """
        return VocabularyHandle(
            values=set(data),
            name=descriptor['name'],
            namespace=descriptor['namespace'],
            label=descriptor.get('label'),
            description=descriptor.get('description')
        )

    def serialize(self, object: VocabularyHandle) -> Tuple[Dict, List]:
        """Serialize the given controlled vocabulary handle.

        Parameters
        ----------
        object: openclean.engine.object.vocabulary.VocabularyHandle
            Object of type that is supported by the serializer.

        Returns
        -------
        tuple of dict and list
        """
        return object.to_dict(), list(object.values)
