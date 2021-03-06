# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Handle and factory implementations for lookup tables (mappings) that are
registered with the object library.
"""

from typing import Dict, List, Optional, Tuple

from openclean.data.mapping import Mapping, StringMatch
from openclean.engine.object.base import ObjectHandle, ObjectFactory


class MappingHandle(ObjectHandle):
    """Handle for a mapping of matched terms that is registered with the object
    library. Extends the base handle with the mapping dictionary.
    """
    def __init__(
        self, mapping: Mapping, name: str, namespace: Optional[str] = None,
        label: Optional[str] = None, description: Optional[str] = None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        mapping: openclean.data.mapping.Mapping
            Mapping (lookup table) of matched terms.
        name: string
            Name for the mapping.
        namespace: string, default=None
            Name of the namespace that this mapping belongs to. By default
            all mappings will be placed in a global namespace (None).
        label: string, default=None
            Optional human-readable name for display purposes.
        description: str, default=None
            Descriptive text for the mapping. This text can for example be
            displayed as a tooltip in a user interface.
        """
        super(MappingHandle, self).__init__(
            name=name,
            namespace=namespace,
            label=label,
            description=description
        )
        self.mapping = mapping


class MappingFactory(ObjectFactory):
    """Factory for mapping handles."""
    def deserialize(self, descriptor: Dict, data: List[Dict]) -> ObjectHandle:
        """Convert an object serialization that was generated by the object
        serializer into a mapping handle.

        Parameters
        ----------
        descriptor: dict
            Dictionary serialization for the mapping descriptor as created by
            the serialize method.
        data: list of dict
            Serialization for the mapping table as created by the serialize
            method.

        Returns
        -------
        openclean.engine.object.mapping.MappingHandle
        """
        # Deserialize the mapping.
        mapping = Mapping()
        for obj in data:
            mapping.add(
                key=obj['key'],
                matches=[StringMatch(term=m['term'], score=m['score']) for m in obj['values']]
            )
        return MappingHandle(
            mapping=mapping,
            name=descriptor['name'],
            namespace=descriptor['namespace'],
            label=descriptor.get('label'),
            description=descriptor.get('description')
        )

    def serialize(self, object: MappingHandle) -> Tuple[Dict, List[Dict]]:
        """Serialize the given mapping handle. Returns a serialized descriptor
        and a list of serializations for the mappings of individual terms in
        the mapping table.

        Parameters
        ----------
        object: openclean.engine.object.mapping.MappingHandle
            Object of type that is supported by the serializer.

        Returns
        -------
        tuple of dict and list
        """
        # Serialize the mapping object.
        data = list()
        for key, values in object.mapping.items():
            data.append({
                'key': key,
                'values': [{'term': m.term, 'score': m.score} for m in values]}
            )
        return object.to_dict(), data
