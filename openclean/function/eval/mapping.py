# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Evaluation functions that wrap mapping functions."""

from typing import Callable, Dict, List, Optional, Union

from openclean.data.types import Value
from openclean.function.eval.base import Eval, EvalFunction, InputColumn
from openclean.function.value.mapping import Lookup as lookup
from openclean.function.value.mapping import Standardize as standardize


class Lookup(EvalFunction):
    """Dictionary lookup function. Uses a mapping dictionary to convert input
    values from the input column(s) to their pre-defined targets.
    """
    def __new__(
        self, columns: Union[InputColumn, List[InputColumn]], mapping: Dict,
        raise_error: Optional[bool] = False,
        default: Optional[Union[Callable, Value]] = None,
        as_string: Optional[bool] = False
    ):
        """Initialize the input columns, mapping dictionary and properties that
        control the behavior of the lookup function.

        Parameters
        ----------
        columns: single input column or list of input columns
            Specifies the column(s) from which values are extracted that are
            then passed to the standardization function.
        mapping: dict
            Mapping of input values to their pre-defined targets
        raise_error: bool, default=False
            Raise ValueError if a value given as argument to the eval function
            is not contained in the mapping.
        default: scalar or callable, default=None
            Default return value for input values that are not contained in the
            mapping (if the raise error flag is False). This can be a function
            that is used to compute a default value from the given input value.
        as_string: bool, optional
            Convert all input values to string before lookup if True.

        Raises
        ------
        ValueError
        """
        return Eval(
            columns=columns,
            func=lookup(
                mapping=mapping,
                raise_error=raise_error,
                default=default,
                as_string=as_string
            )
        )


class Standardize(EvalFunction):
    """Use a mapping dictionary to standardize values from one or more input
    column. For a given value that is extracted from the input column(s), if a
    mapping is defined in the dictionary the mapped value is returned. For all
    other values the original value is returned.
    """
    def __new__(self, columns: Union[InputColumn, List[InputColumn]], mapping: Dict):
        """Initialize the input columns and the translation mapping that is
        used to standardize input values.

        Parameters
        ----------
        columns: single input column or list of input columns
            Specifies the column(s) from which values are extracted that are
            then passed to the standardization function.
        mapping: dict
            Mapping of input values to their pre-defined targets.
        """
        return Eval(
            columns=columns,
            func=standardize(mapping=mapping)
        )
