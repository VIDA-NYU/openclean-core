# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Evaluation functions that wrap mapping functions."""

from typing import Dict, List, Union

from openclean.function.eval.base import Eval, EvalFunction, InputColumn
from openclean.function.eval.domain import Lookup  # noqa: F401
from openclean.function.value.mapping import Standardize as standardize


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
            func=standardize(mapping=mapping),
            is_unary=True
        )
