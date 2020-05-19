# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import jsonschema


"""Json schema for column descriptors and dataset descriptors."""

COLUMN_DESCRIPTOR_SCHEMA = {
    'type': 'object',
    'properties': {
        'name': {'type': 'string'},
        'description': {'type': 'string'}
    },
    'required': ['name']
}


DATASET_DESCRIPTOR_SCHEMA = {
    'type': 'object',
    'properties': {
        'id': {'type': 'string'},
        'name': {'type': 'string'},
        'description': {'type': 'string'},
        'columns': {
            'type': 'array',
            'items': [COLUMN_DESCRIPTOR_SCHEMA]
        },
    },
    'required': ['id', 'columns']
}


class DatasetDescriptor(object):
    """Wrapper around a dataset descriptor dictionary object. This class
    provides access to descriptor property values and their defaults.
    """
    def __init__(self, doc, validate=True):
        """Initialize the dictionary containing the dataset descriptor.
        Validates the document against the dataset schema if the flag is True.
        Raises a ValidationError if validation fails.

        Parameters
        ----------
        doc: dict
            Dictionary containing a dataset descriptor. The dictionary should
            adhere to the defined dataset descriptor schema.
        validate: bool default=True
            Validate the given dictionary against the dataset descriptor schema
            if this flag is True. Raises a ValidationError if the validation
            fails.

        Raises
        ------
        jsonschema.ValidationError
        """
        self.doc = doc
        if validate:
            jsonschema.validate(instance=doc, schema=DATASET_DESCRIPTOR_SCHEMA)

    def columns(self, index=None):
        """Get the list of descriptors for dataset columns or a single column
        if the index is given.

        Parameters
        ----------
        index: int: default=None
            Index of the column that is being returned. If None, the list of
            columns is returned.

        Returns
        -------
        list(openclean.data.downloader.dataset.ColumnDescriptor)
        or
        openclean.data.downloader.dataset.ColumnDescriptor
        """
        if index is not None:
            return ColumnDescriptor(self.doc['columns'][index])
        else:
            return [ColumnDescriptor(col) for col in self.doc['columns']]

    def description(self):
        """Get dataset description. If the value is not set in the descriptor
        an empty string is returned as default.

        Returns
        -------
        string
        """
        return self.doc.get('description', '')

    def identifier(self):
        """Get dataset identifier value.

        Returns
        -------
        string
        """
        return self.doc['id']

    def name(self):
        """Get dataset name. If the value is not set in the descriptor the
        identifier is returned as default.

        Returns
        -------
        string
        """
        return self.doc.get('name', self.identifier())


class ColumnDescriptor(object):
    """Wrapper around a column descriptor dictionary object. This class
    provides access to descriptor property values and their defaults.
    """
    def __init__(self, doc, validate=False):
        """Initialize the dictionary containing the column descriptor.
        Validates the document against the column descriptor schema if the
        flag is True. Raises a ValidationError if validation fails.

        Parameters
        ----------
        doc: dict
            Dictionary containing a column descriptor. The dictionary should
            adhere to the defined column descriptor schema.
        validate: bool default=True
            Validate the given dictionary against the column descriptor schema
            if this flag is True. Raises a ValidationError if the validation
            fails.

        Raises
        ------
        jsonschema.ValidationError
        """
        self.doc = doc
        if validate:
            jsonschema.validate(instance=doc, schema=COLUMN_DESCRIPTOR_SCHEMA)

    def description(self):
        """Get column description. If the value is not set in the descriptor
        an empty string is returned as default.

        Returns
        -------
        string
        """
        return self.doc.get('description', '')

    def name(self):
        """Get column name.

        Returns
        -------
        string
        """
        return self.doc['name']
