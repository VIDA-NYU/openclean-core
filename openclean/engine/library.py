# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""The object library is a repository for storing objects of different types.
These objects include user-defined functions, lookup tables (mappings), and
controlled vocabularies.

Objects within each of the supported types are maintained under a unique name
and namespace.

The library is merely a wrapper around an object stores that are responsible for
maintaining (and potentially persisting) the objects. The library class provides
dedicated methods to access the object stores for different types.
"""

from __future__ import annotations
from typing import Callable, Iterable, List, Optional, Union

from openclean.data.mapping import Mapping
from openclean.data.store.mem import VolatileDataStore
from openclean.engine.object.base import ObjectFactory
from openclean.engine.object.function import FunctionFactory, FunctionHandle, Parameter
from openclean.engine.object.mapping import MappingFactory, MappingHandle
from openclean.engine.object.vocabulary import VocabularyFactory, VocabularyHandle
from openclean.engine.store.base import ObjectStore
from openclean.engine.store.default import DefaultObjectStore


class ObjectLibrary(object):
    """The object library provides access to different types of objects that a
    user can define and register with the library (e.g., user-defined functions,
    lookup tables, etc.).

    The registered objects for each different object type are managed by separate
    object stores. For now, the library only provides (hard-coded) access to the
    object stores for different types of objects. All the functionality for
    creating, retrieving and deleting objects is with the object stores.

    In the future we may want to make this class easier to extend for new object
    types (e.g., create access methods for object stores of known object types
    automatically).
    """
    def __init__(
        self, functions: Optional[ObjectStore] = None,
        lookups: Optional[ObjectStore] = None,
        vocabularies: Optional[ObjectStore] = None
    ):
        """Initialize the stores for the different object types. By default,
        volatile stores are used for those types where the user did not provide
        an object store.

        Parameters
        ----------
        functions: openclean.engine.store.base.ObjectStore, default=None
            Object store for user-defined functions.
        lookups: openclean.engine.store.base.ObjectStore, default=None
            Object store for lookup tables.
        vocabularies: openclean.engine.store.base.ObjectStore, default=None
            Object store for controlled vocabularies.
        """
        self._functions = functions if functions is not None else default_store(FunctionFactory())
        self._lookups = functions if lookups is not None else default_store(MappingFactory())
        self._vocabularies = functions if vocabularies is not None else default_store(VocabularyFactory())

    def eval(
        self, name: Optional[str] = None, namespace: Optional[str] = None,
        label: Optional[str] = None, description: Optional[str] = None,
        columns: Optional[int] = None, collabels: Optional[Union[str, List[str]]] = None,
        outputs: Optional[int] = None, parameters: Optional[List[Parameter]] = None
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
        description: str, default=None
            Descriptive text for the function. This text can for example be
            displayed as tooltips in a front-end.
        columns: int, default=None
            Specifies the number of input columns that the registered function
            operates on. The function will receive exactly one argument for
            each column plus arguments for any additional parameter. The
            column values will be the first arguments that are passed to the
            registered function.
        collabels: string or list of string, default=None
            Display labels for the nput columns. If given the number of values
            has to match the ``columns`` value.
        outputs: int, default=None
            Defines the number of scalar output values that the registered
            function returns. By default it is assumed that the function will
            return a single scalar value.
        parameters: list of openclean.engine.object.function.Parameter,
                default=None
            List of declarations for additional input parameters to the
            registered function.

        Returns
        -------
        openclean.engine.object.function.FunctionHandle
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
                description=description,
                columns=columns,
                collabels=collabels,
                outputs=outputs,
                parameters=parameters
            )
            self._functions.insert_object(object=handle)
            # Return the undecorated function so that it can be used normally.
            return handle
        return register_eval

    def functions(self) -> ObjectStore:
        """Get object store that manages user-defined functions.

        Returns
        -------
        openclean.engine.store.base.ObjectStore
        """
        return self._functions

    def lookup(
        self, mapping: Mapping, name: str, namespace: Optional[str] = None,
        label: Optional[str] = None, description: Optional[str] = None
    ) -> MappingHandle:
        """Create a new lookup table object for the given mapping. Returns the
        handle for the created object.

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

        Returns
        -------
        openclean.engine.object.mapping.MappingHandle
        """
        handle = MappingHandle(
            mapping=mapping,
            name=name,
            namespace=namespace,
            label=label,
            description=description
        )
        self._lookups.insert_object(object=handle)
        return handle

    def lookups(self) -> ObjectStore:
        """Get object store that manages lookup tables.

        Returns
        -------
        openclean.engine.store.base.ObjectStore
        """
        return self._lookups

    def vocabularies(self) -> ObjectStore:
        """Get object store that manages controlled vocabularies.

        Returns
        -------
        openclean.engine.store.base.ObjectStore
        """
        return self._vocabularies

    def vocabulary(
        self, values: Iterable, name: str, namespace: Optional[str] = None,
        label: Optional[str] = None, description: Optional[str] = None
    ) -> VocabularyHandle:
        """Register a controlled vocabulary with the library. Returns the
        handle for the created object.

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

        Returns
        -------
        openclean.engine.object.vocabulary.VocabularyHandle
        """
        handle = VocabularyHandle(
            values=set(values),
            name=name,
            namespace=namespace,
            label=label,
            description=description
        )
        self._vocabularies.insert_object(object=handle)
        return handle


# -- Helper functions ---------------------------------------------------------

def default_store(factory: ObjectFactory) -> DefaultObjectStore:
    """Create an instance of the default object store for the object type that
    is supported by the given factory.

    Parameters
    ----------
    factory: openclean.engine.object.base.ObjectFactory
        Factory for object handles.

    Returns
    -------
    openclean.engine.store.default.DefaultObjectStore
    """
    return DefaultObjectStore(
        identifier='index',
        factory=factory,
        store=VolatileDataStore()
    )
