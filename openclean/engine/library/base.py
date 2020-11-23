# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

from __future__ import annotations
from typing import Any, Callable, List, Optional

from openclean.engine.library.func import FunctionHandle
from openclean.engine.library.parameter import Parameter
from openclean.engine.store.base import ObjectRepository


"""Data type identifier for registered functions."""
DTYPE_FUNC = 'eval'


class ObjectLibrary(object):
    """Registry for commands that can be applied to a single data frame. The
    registry maintains information about available (registered) commands and
    it provides the functionality to add new commands to the registry.
    """
    def __init__(self, store: ObjectRepository):
        """Initialize the dictionary that maintains registered commands."""
        # At this point we should 'scan' for implementations of OpencleanCell
        # classes and add them automatically to the registry.
        # For now we simply add a few string functions for demonstration
        # purposes.
        self.store = store
        # self.eval(namespace='string')(str.lower)
        # self.eval(namespace='string')(str.upper)
        # self.eval(namespace='string')(str.capitalize)

    def eval(
        self, name: Optional[str] = None, namespace: Optional[str] = None,
        label: Optional[str] = None, help: Optional[str] = None,
        columns: Optional[int] = None, outputs: Optional[int] = None,
        parameters: Optional[List[Parameter]] = None
    ) -> Callable:
        """Decorator that adds a new function to the registered set of data
        frame transformers.

        Parameters
        ----------
        name: string, default=None
            Name of the registered function.
        namespace: string, default=None
            Name of the namespace that this function belongs to. By default all
            functions will be placed in a global namespace.
        label: string, default=None
            Optional human-readable name for display purposes.
        help: str, default=None
            Descriptive text for the function. This text can for example be
            displayed as tooltips in a front-end.
        columns: int, default=None
            Specifies the number of input columns that the registered function
            operates on. The function will receive exactly one argument for
            each column plus arguments for any additional parameter. The
            column values will be the first arguments that are passed to the
            registered function.
        outputs: int, default=None
            Defines the number of scalar output values that the registered
            function returns. By default it is assumed that the function will
            return a single scalar value.
        parameters: list of openclean.engine.parameter.Parameter,
                default=None
            List of declarations for additional input parameters to the
            registered function.

        Returns
        -------
        openclean.engine.library.func.FunctionHandle
        """
        def register_eval(func: Callable) -> Callable:
            """Decorator that registeres the given function in the associated
            object registry.
            """
            # Add function together with its metadata to the repository.
            handle = FunctionHandle(
                func=func,
                namespace=namespace,
                name=name,
                label=label,
                help=help,
                columns=columns,
                outputs=outputs,
                parameters=parameters
            )
            self.store.insert_object(
                object=handle,
                name=handle.name,
                dtype=DTYPE_FUNC,
                namespace=handle.namespace
            )
            # Return the undecorated function so that it can be used normally.
            return handle
        return register_eval

    def get(self, name: str, namespace: Optional[str] = None) -> Any:
        """Get an object handle from the associated registry. The object is
        identified by its name and optional namespace. Raises a KeyError if
        the object is unknown.

        Parameters
        ----------
        name: string
            Object name.
        namespace: string, default=None
            Optional namespace identifier.

        Returns
        -------
        any
        """
        return self.store.get_object(name=name, namespace=namespace)
