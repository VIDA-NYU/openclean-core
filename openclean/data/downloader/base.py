# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Abstract base class for repository downloader."""

from abc import ABCMeta, abstractmethod

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
        'primaryKey': {
            'type': 'array',
            'items': {'type': 'string'}
        }
    },
    'required': ['id', 'columns']
}


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
        list(openclean.data.downloader.base.ColumnDescriptor)
        or
        openclean.data.downloader.base.ColumnDescriptor
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

    def primary_key(self):
        """Get list of primary key attributes.

        Returns
        -------
        list(string)
        """
        return self.doc.get('primaryKey')


class RepositoryDownloader(metaclass=ABCMeta):
    """Base class for masterdata repositories. Defines methods for listing the
    datasets that are available for download and for dataset download.
    """
    @abstractmethod
    def datasets(self):
        """Get a description of datasets that are available for download from
        the repository. Returns a list of dataset descriptors.

        Returns
        -------
        list(openclean.data.downloader.base.DatasetDescriptor)
        """
        raise NotImplementedError()

    @abstractmethod
    def download(self, datasets=None, properties=None):
        """Download one or more datasets from a masterdata repository. Returns
        a dictionary that maps the identifier of the downloaded datasets to
        data frames containing the data.

        Parameters
        ----------
        datasets: string or list of string, default=None
            List of identifier for datasets that are downladed. The set of
            valid identifier is depending on the repository.
        properties: dict, default=None
            Additional properties for the download. The set of valid properties
            is dependent on the repository.

        Returns
        -------
        list(
            tuple(
                openclean.data.downloader.base.DatasetDescriptor,
                pandas.DataFrame
            )
        )

        Raises
        ------
        ValueError
        """
        raise NotImplementedError()
