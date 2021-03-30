# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Handle and factory implementations for function objects that are registered
with the object library.

Imports parameter declarations from flowserv. The constructor for each parameter
takes at least the following arguments:

    name: string
        Unique parameter identifier
    index: int, default=0
        Index position of the parameter (for display purposes).
    label: string, default=None
        Human-readable parameter name.
    help: string, default=None
        Descriptive text for the parameter.
    default: any, default=None
        Optional default value.
    required: bool, default=False
        Is required flag.
"""

from flowserv.model.parameter.factory import ParameterDeserializer
from typing import Callable, Dict, List, Optional, Tuple, Union

import dill

from openclean.engine.object.base import ObjectHandle, ObjectFactory

# Import parameter declarations from flowserv.
from flowserv.model.parameter.base import Parameter  # noqa: F401
from flowserv.model.parameter.boolean import Bool  # noqa: F401
from flowserv.model.parameter.enum import Select, Option  # noqa: F401
from flowserv.model.parameter.files import File  # noqa: F401
from flowserv.model.parameter.list import Array  # noqa: F401
from flowserv.model.parameter.numeric import Boundary, Int, Float  # noqa: F401
from flowserv.model.parameter.record import Record  # noqa: F401
from flowserv.model.parameter.string import String  # noqa: F401


"""Default encoding for pickeled functions objects. It seems that CP-1252 is
used as default although I could not find any reference for this in the
documentation.
"""
DEFAULT_ENCODING = 'cp1252'


class FunctionHandle(ObjectHandle):
    """Handle for functions that are registered with the library."""
    def __init__(
        self, func: Callable, name: Optional[str] = None, namespace: Optional[str] = None,
        label: Optional[str] = None, description: Optional[str] = None,
        columns: Optional[int] = None, collabels: Optional[Union[str, List[str]]] = None,
        outputs: Optional[int] = None, parameters: Optional[List[Parameter]] = None
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
        description: str, default=None
            Descriptive text for the function. This text can for example be
            displayed as a tooltip in a user interface.
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
        """
        super(FunctionHandle, self).__init__(
            name=name if name is not None else func.__name__,
            namespace=namespace,
            label=label,
            description=description
        )
        self.func = func
        self.columns = columns if columns is not None else 1
        self.outputs = outputs if outputs is not None else 1
        self.parameters = parameters if parameters is not None else list()
        # Get the column labels from the function code descriptor (if possible).
        if not collabels:
            try:
                collabels = list(func.__code__.co_varnames)[:self.columns]
            except AttributeError:
                pass
        elif len(collabels) != self.columns:
            raise ValueError('expected {} column labels'.format(self.columns))
        self.collabels = collabels
        # The function handle can be used as a substitue for the registered
        # function. Use the function name as the name for the handle.
        self.__name__ = func.__name__

    def __call__(self, *args, **kwargs):
        """Make the function handle callable so that it can be used as a
        substitute for the registered function.
        """
        return self.func(*args, **kwargs)


class FunctionFactory(ObjectFactory):
    """Factory for function objects. Uses dill to serialize functions."""
    def deserialize(self, descriptor: Dict, data: str) -> ObjectHandle:
        """Convert an object serialization that was generated by the object
        serializer into a function handle.

        Parameters
        ----------
        descriptor: dict
            Dictionary serialization for the object descriptor.
        data: string
            Serialization for the function declaration.

        Returns
        -------
        openclean.engine.object.function.FunctionHandle
        """
        return FunctionHandle(
            func=dill.loads(data.encode(encoding=DEFAULT_ENCODING)),
            name=descriptor['name'],
            namespace=descriptor['namespace'],
            label=descriptor.get('label'),
            description=descriptor.get('description'),
            columns=descriptor['columns'],
            collabels=descriptor['columnLabels'],
            outputs=descriptor['outputs'],
            parameters=[
                ParameterDeserializer.from_dict(obj) for obj in descriptor['parameters']
            ]
        )

    def serialize(self, object: ObjectHandle) -> Tuple[Dict, str]:
        """Serialize the given function handle. Returns the serialized function
        descriptor and the serialized function declaration. The function is
        serialized using dill and the result converted into a string.

        Parameters
        ----------
        object: openclean.engine.object.function.FunctionHandle
            Object of type that is supported by the serializer.

        Returns
        -------
        tuple of dict and string
        """
        descriptor = object.to_dict()
        descriptor['columns'] = object.columns
        descriptor['columnLabels'] = object.collabels
        descriptor['outputs'] = object.outputs
        descriptor['parameters'] = [p.to_dict() for p in object.parameters]
        data = dill.dumps(object.func).decode(encoding=DEFAULT_ENCODING)
        return descriptor, data
