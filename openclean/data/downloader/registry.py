# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

import importlib
import jsonschema
import json

from openclean.data.downloader.base import RepositoryDownloader

import openclean.config as config


"""Json schema for repository handles."""

REPOSITORY_HANDLE_SCHEMA = {
    'type': 'object',
    'properties': {
        'id': {'type': 'string'},
        'name': {'type': 'string'},
        'description': {'type': 'string'},
        'loaderModule': {'type': 'string'},
        'loaderClass': {'type': 'string'}
    },
    'required': ['id', 'loaderModule', 'loaderClass']
}


class RepositoryHandle(RepositoryDownloader):
    def __init__(self, doc, validate=True):
        """Initialize the dictionary containing the repository handle.
        Validates the document against the repository handle schema if the
        validate flag is True. Raises a ValidationError if validation fails.

        Parameters
        ----------
        doc: dict
            Dictionary containing a repository handle. The dictionary should
            adhere to the defined repository handle schema.
        validate: bool default=True
            Validate the given dictionary against the repository handle schema
            if this flag is True. Raises a ValidationError if the validation
            fails.

        Raises
        ------
        jsonschema.ValidationError
        """
        self.doc = doc
        if validate:
            jsonschema.validate(instance=doc, schema=REPOSITORY_HANDLE_SCHEMA)
        # Create instance of dataset downloader for the repository.
        module = importlib.import_module(doc['loaderModule'])
        self.loader = getattr(module, doc['loaderClass'])()

    def datasets(self):
        """Get a description of datasets that are available for download from
        the repository.

        Returns
        -------
        dict
        """
        return self.loader.datasets()

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
        dict

        Raises
        ------
        ValueError
        """
        return self.loader.download(datasets=datasets, properties=properties)


def repositories(identifier=None):
    """Get list of all registered repositories if the identifier is None. If an
    identifier is given, the handle for the identified repository is returned.
    Raises a ValueError if an unknown identifier is given.

    Parameters
    ----------
    identifier: string, default=None
        Unique identifier of a registered download repository.

    Returns
    -------
    list(openclean.data.downloader.registry.RepositoryHandle)
    or
    openclean.data.downloader.registry.RepositoryHandle

    Raises
    ------
    ValueError
    """
    # Read document containing array of registered repositories.
    filename = config.REPOSITORY_REGISTRY()
    with open(filename, 'r') as f:
        doc = json.load(f)
    # Depending on whether a repository identifier is specified or not either
    # return a list of all repository handles in the document or only the
    # handle for the identified repository.
    if identifier is not None:
        for rh in doc:
            if rh.get('id') == identifier:
                return RepositoryHandle(rh)
        # No repository with the given identifier was found.
        raise ValueError('unknown repository {}'.format(identifier))
    else:
        return [RepositoryHandle(rh) for rh in doc]
