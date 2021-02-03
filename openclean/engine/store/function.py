# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Persistent repository for library functions. Instantiates the default object
store with a persistent data store for registered functions.
"""

from typing import List, Optional

import os

from openclean.data.store.fs import FileSystemJsonStore
from openclean.engine.object.function import FunctionFactory, FunctionHandle
from openclean.engine.store.default import DefaultObjectStore

import openclean.config as config


class FunctionRepository(DefaultObjectStore):
    """Object store for library functions. Persists all user-defined functions
    on disk using a file system data store. The files are stored under the
    openclean home directory if no base directory is specififed.
    """
    def __init__(
        self, basedir: Optional[str] = None,
        defaults: Optional[List[FunctionHandle]] = None
    ):
        """Initialize the base directory where all object files are stored.
        Stores all files under the openclean home directory if no base directory
        is specified.

        The base directory is creted
        Parameters
        ----------
        basedir: string, default=None
            Base directory for object files.
        defaults: list of openclean.engine.object.function.FunctionHandle, default=None
            Default function library.
        """
        if basedir is None:
            basedir = os.path.join(config.DATADIR(), 'functions')
        super(FunctionRepository, self).__init__(
            identifier='.index',
            factory=FunctionFactory(),
            store=FileSystemJsonStore(basedir),
            defaults=defaults
        )
