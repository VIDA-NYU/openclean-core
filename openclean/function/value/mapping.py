# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""The mapping operator that returns a dictionary that contains a mapping of
original values in a data frame column(s) to results of applying a given value
function on them.

Lookup functions represent mappings using dictionaries.
"""

from typing import Callable, Dict, Optional, Union

import pandas as pd

from openclean.data.sequence import Sequence
from openclean.data.types import Columns, Value
from openclean.function.value.base import (
    CallableWrapper, PreparedFunction, ValueFunction
)
from openclean.function.value.cond import ConditionalStatement
from openclean.util.core import scalar_pass_through


# -- Functions ----------------------------------------------------------------

def mapping(df: pd.DataFrame, columns: Columns, func: Union[Callable, ValueFunction]) -> Dict:
    """Get the mapping of values that are modified by a given value function.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.
    func: callable or openclean.function.value.base.ValueFunction
        Callable or value function that accepts a single value as the argument.

    Returns
    -------
    dict

    Raises
    ------
    ValueError
    """
    if not isinstance(func, ValueFunction):
        func = CallableWrapper(func=func)
    values = Sequence(df=df, columns=columns)
    result = dict()
    if not func.is_prepared():
        func = func.prepare(values)
    for value in values:
        if value not in result:
            result[value] = func.eval(value)
    return result


def replace(predicate: Callable, value: Value) -> ConditionalStatement:
    """Return an instance of the Replace class for the given arguments.

    Parameters
    ----------
    predicate: callable
        Predicate that is evalauated on input values.
    value: scalar or tuple
        Replacement value for inputs that satisfy the predicate.

    Returns
    -------
    openclean.function.value.mapping.Replace
    """
    return ConditionalStatement(
        predicate=predicate,
        stmt=value,
        elsestmt=scalar_pass_through
    )


# -- Value functions ----------------------------------------------------------

class Lookup(PreparedFunction):
    """Dictionary lookup function. Uses a mapping dictionary to convert given
    input values to their pre-defined targets.
    """
    def __init__(
        self, mapping: Dict, raise_error: Optional[bool] = False,
        default: Optional[Union[Callable, Value]] = None,
        as_string: Optional[bool] = False
    ):
        """Initialize the mapping dictionary and properties that control the
        behavior of the lookup function.

        Parameters
        ----------
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
        self.mapping = mapping
        self.raise_error = raise_error
        self.default = default
        self.as_string = as_string

    def eval(self, value: Value) -> Value:
        """Return the defined target value for a given lookup value.

        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        any
        """
        key = value
        # Convert the lookup key to string if the respective flag is True.
        if self.as_string:
            key = str(key)
        if key not in self.mapping:
            # Raise an error if the raise error flag is True and the given key
            # value is unknown. Otherwise, return the defined target value or
            # the default value (for missing keys).
            if self.raise_error:
                raise KeyError('unknown value {}'.format(value))
            if callable(self.default):
                return self.default(value)
            return self.default
        return self.mapping.get(key)


class Standardize(PreparedFunction):
    """Use a mapping dictionary to standardize values. For a given value, if a
    mapping is defined in the dictionary the mapped value is returned. For all
    other values the original value is returned.
    """
    def __init__(self, mapping: Dict):
        """Initialize the translation mapping that is used to standardize input
        values.

        Parameters
        ----------
        mapping: dict
            Mapping of input values to their pre-defined targets.
        """
        self.mapping = mapping

    def eval(self, value: Value) -> Value:
        """Return the defined target value for a given lookup value. If the
        given value is not included in the standardization mapping it will be
        returned as is.


        Parameters
        ----------
        value: scalar
            Scalar value in a data stream.

        Returns
        -------
        any
        """
        return self.mapping.get(value, value)
