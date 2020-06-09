# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Value mapping and replacement operators."""

import pandas as pd

from openclean.data.util import to_lookup
from openclean.function.eval.base import Eval
from openclean.function.value.lookup import Lookup


# -- Value mapping ------------------------------------------------------------

class Map(object):
    """Evaluation function that maps single values (scalar or tuple) to single
    values (scalar or tuple) based on a given lookup mapping.
    """
    def __new__(cls, columns, mapping, raise_error=False):
        """Initialize the constant return value. Define the behavior for
        optional type conversion.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        mapping: dict or openclean.function.value.lookup.Lookup
            Mapping from input to output values. The type of the keys in the
            mapping dictionary are expected to match the value type that is
            defined by the columns list.
        raise_error: bool, optional
            Controls the behavior of a lookup table that is created if the
            mapping argument is a dictionary. If the flag is True the default
            behavior of the created lookup for unknonwn keys is to raise an
            error. Otherwise, the input value is returned as output.
        """
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
            mapping = Lookup(mapping, raise_error=raise_error)
        return Eval(func=mapping, columns=columns)
