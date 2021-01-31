# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Datatype converter for profiling purposes. The converter returns a converted
value together with the type label for datatype counting and (min,max)
computations.
"""

from datetime import datetime
from typing import Callable, List, Tuple, Type

from openclean.data.types import Scalar
from openclean.function.value.datatype import (
    to_datetime, to_float, to_int, to_string
)


class DatatypeConverter(object):
    """Converter for scalar values that is used for profiling purposes. The
    converter maintains a list of datatype converters (callables) each of which
    is assigned to a raw type and a type label.

    Converters and their associated label are used to represent the associated
    raw data types. That is, converters are expected to return None for values
    that cannot be converted to the respective raw data type.

    The datatype converter returns the converted value and type label for the
    first converter that represents a raw data type to wich the given value
    could be converted, i.e., that accepted the given value. If a value has a
    raw type that matches the raw type of one of the converters the value
    itself and the label for that respective converter is returned (Issue #45).

    Note that the raw type for a given converter can be None. In this case
    we ignore this converter when checking whether the raw type of an input
    value matches the represented type of the converter, The converter may
    still be used in case the raw type of an input value does not match the
    raw types of any of the other converters and we attempt to cast the value.

    If no converter accepted the original value and a default type label is
    returned.
    """
    def __init__(
        self, datatypes: List[Tuple[Type, str, Callable]], default_label: str
    ):
        """Initialize the list of value converters and their associated raw
        type and type labels.

        Parameters
        ----------
        datatypes: list of tuples
            Datatypes are represented by a tuple of raw data type, type label,
            and a callable function that is used to attempt to cast a given
            value to the datatype that is represented by the converter.
        default_label: string
            Default label for values that do not match any of the given data
            type converters.
        """
        self.datatypes = datatypes
        self.default_label = default_label
        # Create a mapping of raw types to type labels for all converter that
        # have a raw data type associated with them.
        self.rawtypes = [(rt, lbl) for rt, lbl, _ in datatypes if rt]

    def cast(self, value: Scalar) -> Scalar:
        """Convert a given value. Returns the resulting value without the type
        label.

        Parameters
        ----------
        value: scalar
            Value that is converted for data type detection.

        Returns
        -------
        scalar
        """
        val, _ = self.convert(value)
        return val

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
        # Check if the given value has a type that matches any of the raw types
        # of the associated converters.
        for rawtype, label in self.rawtypes:
            if isinstance(value, rawtype):
                return value, label
        # Attempt to convert the given value if it did not match the raw type
        # of any of the converters.
        for _, label, f in self.datatypes:
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
    openclean.profiling.datatype.convert.DatatypeConverter
    """
    return DatatypeConverter(
        datatypes=[
            (int, 'int', to_int),
            (float, 'float', to_float),
            (datetime, 'date', to_datetime),
            (None, 'str', to_string)
        ],
        default_label='unknown'
    )
