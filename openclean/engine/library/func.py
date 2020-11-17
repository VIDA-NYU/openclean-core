# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Handles and serializer for funciton objects that are registered with the
openclean library.
"""

from __future__ import annotations
from typing import Callable, Dict, List, Optional

import dill

from flowserv.model.parameter.factory import ParameterDeserializer as ps

from openclean.engine.library.parameter import Parameter
from openclean.engine.store.serialized import ObjectSerializer, JsonSerializer


"""Default encoding for pickeled functions objects. It seems that CP-1252 is
used as default although I could not find any reference for this in the
documentation.
"""
DEFAULT_ENCODING = 'cp1252'


class FunctionHandle:
    """Handle for functions that are registered with the openclean function
    library.
    """
    def __init__(
        self, func: Callable, name: Optional[str] = None, namespace: Optional[str] = None,
        label: Optional[str] = None, help: Optional[str] = None,
        columns: Optional[int] = None, outputs: Optional[int] = None,
        parameters: Optional[List[Parameter]] = None
    ):
        """Initialize the object properties.

        Parameters
        ----------
        func: callable
            Registered function object.
        name: string, default=None
            Name of the registered function.
        namespace: string, default=None
            Name of the namespace that this function belongs to. By default all
            functions will be placed in a global namespace (None).
        label: string, default=None
            Optional human-readable name for display purposes.
        help: str, default=None
            Descriptive text for the function. This text can for example be
            displayed as a tooltip in a user interface.
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

        """
        self.func = func
        self.name = name if name is not None else func.__name__
        self.namespace = namespace
        self.label = label
        self.help = help
        self.columns = columns if columns is not None else 1
        self.outputs = outputs if outputs is not None else 1
        self.parameters = parameters if parameters is not None else list()

    @staticmethod
    def from_dict(doc: Dict) -> FunctionHandle:
        """Create an instance of the function handle from a dictionary
        serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization for a function handle as created by the
            `to_dict()` method.

        Returns
        -------
        openclean.engine.library.func.FunctionHandle
        """
        return FunctionHandle(
            func=dill.loads(doc['func'].encode(encoding=DEFAULT_ENCODING)),
            name=doc['name'],
            namespace=doc['namespace'],
            label=doc['label'],
            help=doc['help'],
            columns=doc['columns'],
            outputs=doc['outputs'],
            parameters=[ps.from_dict(obj) for obj in doc['parameters']]
        )

    def to_descriptor(self) -> Dict:
        """Create a dictionary serialization of the function handle that does
        not contain the serialization of the function object itself. Descriptors
        are used for function listing in a user-interface, for example.

        Returns
        -------
        dict
        """
        return {
            'name': self.name,
            'namespace': self.namespace,
            'label': self.label,
            'help': self.help,
            'columns': self.columns,
            'outputs': self.outputs,
            'parameters': [p.to_dict() for p in self.parameters]
        }

    def to_dict(self) -> Dict:
        """Create a dictionary serialization of the function handle. Uses the
        dill package to serialize function objects.

        Returns
        -------
        dict
        """
        doc = self.to_descriptor()
        doc['func'] = dill.dumps(self.func).decode(encoding=DEFAULT_ENCODING)
        return doc


class FunctionSerializer(ObjectSerializer):
    """Serializer for function objects. Uses dill to serialize functions and
    the Json serializer to created serialized object strings.
    """
    def __init__(self, serializer: Optional[ObjectSerializer] = None):
        """Initialize the serializer that transforms dictionary serializations
        for function handles into strings and vice versa. By default, the
        Json serializer is used.

        Parameters
        ----------
        serializer: openclean.engine.store.serialized.ObjectSerializer,
                default=None
            Serializer for dictionary serializations of function handles.
        """
        self.serializer = serializer if serializer is not None else JsonSerializer()

    def deserialize(self, value: str) -> FunctionHandle:
        """Convert an object serialization that was generated by the object
        serializer into function handle.

        Parameters
        ----------
        value: string
            Serialized object string created by this serializer.

        Returns
        -------
        openclean.engine.library.func.FunctionHandle
        """
        return FunctionHandle.from_dict(self.serializer.deserialize(value))

    def serialize(self, object: FunctionHandle) -> str:
        """Serialize the given function handle.

        Parameters
        ----------
        object: openclean.engine.library.func.FunctionHandle
            Object of type that is supported by the serializer.

        Returns
        -------
        string
        """
        return self.serializer.serialize(object.to_dict())
