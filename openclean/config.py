# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper functions for accessing configuration parameters for openclean that
are maintained in environment variables.
"""

from pathlib import Path

import os


"""Environment variables that maintain configuration parameters."""
# Directory for raw data files.
ENV_DATA_DIR = 'OPENCLEAN_DATA_DIR'
# Download directory
ENV_MASTERDATA_DIR = 'OPENCLEAN_MASTERDATA_DIR'


def DATADIR():
    """Get directory where raw data files are maintained.

    Returns
    -------
    string
    """
    default_value = os.path.join(str(Path.home()), '.openclean', 'data')
    return os.environ.get(ENV_DATA_DIR, default_value)


def MASTERDATADIR():
    """Get directory where master data repositories are maintained.

    Returns
    -------
    string
    """
    default_value = os.path.join(str(Path.home()), '.openclean', 'masterdata')
    return os.environ.get(ENV_MASTERDATA_DIR, default_value)
