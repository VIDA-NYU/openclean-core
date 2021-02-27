# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Helper functions for accessing configuration parameters for openclean that
are maintained in environment variables.
"""

from appdirs import user_cache_dir
from flowserv.controller.worker.factory import WorkerFactory, convert_config
from typing import Dict, List, Optional

import json
import os
import yaml


"""Environment variables that maintain configuration parameters."""
# Directory for raw data files.
ENV_DATA_DIR = 'OPENCLEAN_DATADIR'
# Number of parallel threads to use.
ENV_THREADS = 'OPENCLEAN_THREADS'
# Configuration file containing the configuration for flowServ workers.
ENV_WORKERS = 'OPENCLEAN_WORKERS'


def DATADIR() -> str:
    """Get directory where raw data files are maintained.

    Returns
    -------
    string
    """
    default_value = os.path.join(user_cache_dir(appname=__name__.split('.')[0]), 'data')
    return os.environ.get(ENV_DATA_DIR, default_value)


def THREADS() -> int:
    """Get number of parallel threads. BY default a value of 1 is used. The
    default value is returned if the environment variable 'OPENCLEAN_THREADS'
    is not set or contains an invalid value (i.e., non-numeric or smaller than
    one).

    Returns
    -------
    string
    """
    try:
        threads = int(os.environ.get(ENV_THREADS, 1))
    except ValueError:
        threads = 1
    return 1 if threads < 1 else threads


def WORKERS(var: Optional[str] = None, env: Optional[Dict] = None) -> WorkerFactory:
    """Create a worker factory for serial workflow execution from a configuration
    file.

    By default, the configuration is read from the file that is referenced by
    the environment variable `OPENCLEAN_WORKERS`. If the optional environment
    variable `var` is given and references an existing file, the configurations
    from that file will override the default configuration.

    Parameters
    ----------
    var: string, default=None
        Optional name for an environment variable that references a file with
        flowServ worker configurations that will override the values in the
        default configuration.
    env: dict, default=None
        Optional environment variables that override the system-wide
        settings., defualt=None

    Returns
    -------
    flowserv.controller.serial.worker.factory.WorkerFactory
    """
    vars = dict(os.environ)
    if env:
        vars.update(env)
    worker_conf = convert_config(read_list(vars.get(ENV_WORKERS)))
    if var:
        worker_conf.update(convert_config(read_list(vars.get(var))))
    return WorkerFactory(config=worker_conf)


# -- Helper Functions ---------------------------------------------------------

def read_list(filename: str) -> List:
    """Read a list object from a given file.

    The file type is guessed from the file suffix. If the file ends with '.json'
    it is assumed to be in Json format. Otherwise, Yaml format is assumed by
    default.

    If the filename is None or empty an empty list is returned.

    Parameters
    ----------
    filename: string
        Path to file on disk.

    Returns
    -------
    list
    """
    # Return an empty list of the filename is not given
    if not filename:
        return list()
    # Guess the file type from the name suffix. By default, Yaml is assumed.
    if filename.endswith('.json'):
        with open(filename, 'r') as f:
            return json.load(f)
    else:
        with open(filename, 'r') as f:
            return yaml.load(f.read(), Loader=yaml.FullLoader)
