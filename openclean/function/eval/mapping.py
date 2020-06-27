# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Value mapping and replacement operators."""

import pandas as pd

from openclean.data.column import select_clause
from openclean.data.util import to_lookup
from openclean.function.eval.base import EvalFunction
from openclean.function.value.base import ValueFunction
from openclean.function.value.lookup import Lookup


# -- Value mapping ------------------------------------------------------------

class Map(EvalFunction):
    """Evaluation function that maps single values (scalar or tuple) to single
    values (scalar or tuple) based on a given lookup mapping.
    """
    def __init__(
        self, columns=None, mapping=None, raise_error=False, colidx=None
    ):
        """Initialize the constant return value. Define the behavior for
        optional type conversion.

        Parameters
        ----------
        columns: int, string, or list(int or string), default=None
            Single column or list of column index positions or column names.
        mapping: pd.DataFrame, dict or openclean.function.value.lookup.Lookup,
                default=None
            Mapping from input to output values. The type of the keys in the
            mapping dictionary are expected to match the value type that is
            defined by the columns list.
        raise_error: bool, optional
            Controls the behavior of a lookup table that is created if the
            mapping argument is a dictionary. If the flag is True the default
            behavior of the created lookup for unknonwn keys is to raise an
            error. Otherwise, the input value is returned as output.
        colidx: list(int), default=None
            Prepared list of index positions for columns.
        """
        # The column index is given only for instances that are prepared.
        if colidx is None:
            # If the column index is not given the function is not prepared
            # yet. We check whether the given mapping is a value function or
            # can be converted into one.
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
            if not isinstance(mapping, ValueFunction):
                raise ValueError('could not convert mapping to value function')
        self.columns = columns
        self.mapping = mapping
        self.raise_error = raise_error
        self._colidx = colidx

    def eval(self, values):
        """Get mapping for cell values in the lookup columns.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        if len(self._colidx) == 1:
            key = values[self._colidx[0]]
        else:
            key = [values[i] for i in self._colidx]
        return self.mapping.eval(key)

    def prepare(self, df):
        """Get index positions of the lookup columns for the schema of the
        given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        _, colidx = select_clause(
            df=df,
            columns=self.columns if self.columns else list(df.columns)
        )
        return Map(
            columns=self.columns,
            mapping=self.mapping,
            colidx=colidx,
            raise_error=self.raise_error
        )
