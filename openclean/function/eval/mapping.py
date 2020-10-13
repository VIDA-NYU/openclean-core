# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Value mapping and replacement operators."""

import pandas as pd

from openclean.data.util import to_lookup
from openclean.function.base import scalar_pass_through
from openclean.function.eval.base import Eval
from openclean.function.value.base import ValueFunction
from openclean.function.value.cond import ConditionalStatement
from openclean.function.value.mapping import Lookup


# -- Value mapping ------------------------------------------------------------

class Map(Eval):
    """Evaluation function that maps single values (scalar or tuple) to single
    values (scalar or tuple) based on a given lookup mapping.
    """
    def __init__(
        self, columns=None, mapping=None, raise_error=False,
        default_value=None, as_string=False
    ):
        """Initialize the constant return value. Define the behavior for
        optional type conversion.

        Parameters
        ----------
        columns: list, tuple, or openclean.function.eval.base.EvalFunction
            Evaluation function to extract values from data frame rows. This
            can also be a list or tuple of evaluation functions or a list of
            column names or index positions.
        mapping: pd.DataFrame, dict or openclean.function.value.mapping.Lookup,
                default=None
            Mapping from input to output values. The type of the keys in the
            mapping dictionary are expected to match the value type that is
            defined by the columns list.
        raise_error: bool, optional
            Controls the behavior of a lookup table that is created if the
            mapping argument is a dictionary. If the flag is True the default
            behavior of the created lookup for unknonwn keys is to raise an
            error. Otherwise, the input value is returned as output.
        default_value: scalar, default=None
            Default return value for input values that are not contained in the
            mapping (if the raise error flag is False).
        as_string: bool, optional
            Convert all input values to string before lookup if True.
        """
        # If the mapping is not a value function we try to convert it into one.
        if isinstance(mapping, pd.DataFrame):
            # If the mapping is a data frame create a mapping from it. The
            # number of columns has to be even since the data frame will
            # be split in the middle between source and target columns.
            if len(mapping.columns) % 2 != 0:
                raise ValueError('cannot convert data frame to mapping')
            mid = len(mapping.columns) // 2
            key_columns = list(range(mid))
            target_columns = list(range(mid, len(mapping.columns)))
            mapping = to_lookup(mapping, key_columns, target_columns)
        if isinstance(mapping, dict):
            # If the mapping is a dictionary wrap it in a lookup table.
            mapping = Lookup(
                mapping,
                raise_error=raise_error,
                default_value=default_value,
                as_string=as_string
            )
        if not isinstance(mapping, ValueFunction):
            raise ValueError('could not convert mapping to value function')
        super(Map, self).__init__(columns=columns, func=mapping, is_unary=True)


class Replace(Eval):
    """Conditional replace statement. Evaluates a given predicate on row values.
    Depending on whether the predicate is satisfied or not one of two given
    statements are being executed.
    """
    def __init__(self, columns, predicate, stmt, elsestmt=scalar_pass_through):
        """Initialize the constant return value. Define the behavior for
        optional type conversion.

        Parameters
        ----------
        columns: list, tuple, or openclean.function.eval.base.EvalFunction
            Evaluation function to extract values from data frame rows. This
            can also be a list or tuple of evaluation functions or a list of
            column names or index positions.
        predicate: callable or scalar
            Predicate that is evaluated on the input values.
        stmt: callable or scalar
            Statement that is evaluated for values that satisfy the predicate.
        elsestmt: callable or scalar, default=scalar_pass_through
            Statement that is evaluated for values that do not satisfy the
            predicate.
        """
        super(Replace, self).__init__(
            columns=columns,
            func=ConditionalStatement(
                predicate=predicate,
                stmt=stmt,
                elsestmt=elsestmt
            ),
            is_unary=True
        )
