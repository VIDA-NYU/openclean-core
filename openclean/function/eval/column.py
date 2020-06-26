# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Evaluate function that returns values from columns in a data frame row."""

from openclean.data.column import select_clause
from openclean.function.eval.base import EvalFunction


class Col(EvalFunction):
    """Evaluation function that returns value(s) from one or more column(s) in
    the data frame row.
    """
    def __init__(self, columns, colidx=None):
        """Initialize the source column(s).

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        colidx: list(int), default=None
            Prepared list of index positions for columns.
        """
        self.columns = columns
        self.colidx = colidx

    def eval(self, values):
        """Get value from the lookup columns.

        Parameters
        ----------
        values: pandas.core.series.Series
            Row in a pandas data frame.

        Returns
        -------
        scalar or tuple
        """
        if len(self.colidx) == 1:
            return values[self.colidx[0]]
        else:
            return [values[i] for i in self.colidx]

    def is_prepared(self):
        """The function is prepared if the column index is not None.

        Returns
        -------
        bool
        """
        return self.colidx is not None

    def prepare(self, df):
        """Get index positions of the value columns for the schema of the
        given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        openclean.function.eval.base.EvalFunction
        """
        _, colidx = select_clause(df, columns=self.columns)
        return Col(columns=self.columns, colidx=colidx)
