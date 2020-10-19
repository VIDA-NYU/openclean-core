# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Datatype converter for profiling purposes. The converter returns a converted
value together with the type label for datatype counting and (min,max)
computations.
"""

from typing import Callable, List, Tuple

from openclean.data.types import Scalar
from openclean.function.value.datatype import to_datetime, to_float, to_int


class DatatypeConverter(object):
    """Converter for scalar values that is used for profiling purposes. The
    converter maintains a list of datatype converters (callables) each of which
    is assigned to a type label. Converters and their associated label are used
    to represent data types. Converters are expected to return None for values
    that do not belong to the respective data type.

    The datatype converter returns the converted value and type label for the
    first converter that did not return None when called with a given value.
    If no converter returned None the original value and a default label is
    returned.
    """
    def __init__(
        self, datatypes: List[Tuple[str, Callable]], default_label: str
    ):
        """Initialize the list of value converters and their associated type
        labels.

        Parameters
        ----------
        datatypes: list of tuples
            Datatypes are represented by a tuple of a callable function and a
            type label. the function is used to attemt to cast a given value to
            the datatype that is represented by the converter.
        default_label: string
            Default label for values that do not match any of the given data
            type converters.
        """
        self.datatypes = datatypes
        self.default_label = default_label

    def convert(self, value: Scalar) -> Tuple[Scalar, str]:
        """Convert a given value. Returns a tuple of converted value and type
        label.

        Parameters
        ----------
        value: scalar
            Value that is converted for data type detection.

        Returns
        -------
        tuple of scalar and string (type label)
        """
        for label, f in self.datatypes:
            val = f(value)
            if val is not None:
                return val, label
        return value, self.default_label


def DefaultConverter() -> DatatypeConverter:
    """Get an instance of the default data type converter for data profiling.
    the default converter distinguishes between integer, float, datetime, and
    text.

    Returns
    -------
    openclean.profiling.datatype.DatatypeConverter
    """
    return DatatypeConverter(
        datatypes=[
            ('int', to_int),
            ('float', to_float),
            ('date', to_datetime)
        ],
        default_label='text'
    )
