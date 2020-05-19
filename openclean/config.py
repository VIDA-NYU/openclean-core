# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper functions for accessing configuration parameters for openclean that
are maintained in environment variables.
"""

import os
import pkg_resources as pkg


"""Environment variables that maintain configuration parameters."""
# Directory for raw data files.
ENV_DATA_DIR = 'OPENCLEAN_DATA_DIR'
# Download directory
ENV_MASTERDATA_DIR = 'OPENCLEAN_MASTERDATA_DIR'
# Repository registry file
ENV_REPOSITORY_REGISTRY = 'OPENCLEAN_REPOSITORY_REGISTRY'


def DATADIR():
    """Get directory where raw data files are maintained.

    Returns
    -------
    string
    """
    return get_variable(ENV_DATA_DIR, raise_error=True)


def MASTERDATADIR():
    """Get directory where master data repositories are maintained.

    Returns
    -------
    string
    """
    return get_variable(ENV_MASTERDATA_DIR, raise_error=True)


def REPOSITORY_REGISTRY():
    """Get path to the repository registration file.

    Returns
    -------
    string
    """
    # Use the value in the respective environment variable by default. If the
    # variable is not set use the default repository registration that is part
    # of the openclean package.
    filename = get_variable(ENV_REPOSITORY_REGISTRY, raise_error=False)
    if not filename:
        pkg_name = __package__.split('.')[0]
        resource_path = 'data/downloader/registry.json'
        filename = pkg.resource_filename(pkg_name, resource_path)
    return filename


# -- Helper Methods -----------------------------------------------------------

def get_variable(name, default_value=None, raise_error=None):
    """Get the value for the given  environment variable. Raises a
    MissingConfigurationError if the raise_error flag is True and the variable
    is not set. If the raise_error flag is False and the environment variables
    is not set then the default value is returned.

    Parameters
    ----------
    name: string
        Environment variable name
    default_value: string, optional
        Default value if variable is not set and raise_error flag is False
    raise_error: bool
        Flag indicating whether an error is raised if the environment variable
        is not set (i.e., None or and empty string '')

    Returns
    -------
    string

    Raises
    ------
    ValueError
    """
    value = os.environ.get(name)
    if value is None or value == '':
        if raise_error:
            raise ValueError('missing configuration for {}'.format(name))
        else:
            value = default_value
    return value
