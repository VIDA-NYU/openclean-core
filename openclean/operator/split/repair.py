# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2020 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Data frame transformation operator that updates values in columns of a data
frame. This operator is similar to the update operator. The difference is that
the operator allows to catch errors raised by the update function. The repair
operator returns two data frames, one that contains the successfully updated
rows and one that contains the rows for which the update failed.
"""

import pandas as pd

from openclean.data.select import select_clause
from openclean.operator.base import DataFrameSplitter
from openclean.operator.transform.update import get_update_function


# -- Functions ----------------------------------------------------------------

def repair(df, columns, func, exceptions=None):
    """Update function for data frames that keeps track of data frame rows for
    which the update failed. The update function is executed for each data
    frame row.  This operator returns two data frames, one that contains the
    successfully modified rows and one that contains the rows for which the
    update failed.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string), optional
        Single column or list of column index positions or column names.
    func: callable or openclean.function.eval.base.EvalFunction
        Callable that accepts a data frame row as the only argument and outputs
        a (modified) list of value(s).
    exceptions: Error or list(Error)
        Exception(s) which will be caught from the update function. Rows for
        which the update function raises one of the exceptions in this list
        will be placed in the data frame with failed rows.

    Returns
    -------
    pandas.DataFrame, pandas.DataFrame

    Raises
    ------
    ValueError
    """
    op = Repair(columns=columns, func=func, exceptions=exceptions)
    return op.split(df)


# -- Operators ----------------------------------------------------------------

class Repair(DataFrameSplitter):
    """Data frame splitter that updates values in data frame column(s) using
    a given update function. The function is executed for each row and the
    resulting values replace the original cell values in the row for all listed
    columns (in their order of appearance in the columns list). Rows for which
    the update function fails are placed in a separate data frame that is
    being returned by the operator as part of the split result.
    """
    def __init__(self, columns, func, exceptions=None):
        """Initialize the list of updated columns, the update function, and the
        list of exceptions that are being caught for failed updates.

        Parameters
        ----------
        columns: int, string, or list(int or string), optional
            Single column or list of column index positions or column names.
        func: callable or openclean.function.eval.base.EvalFunction
            Callable that accepts a data frame row as the only argument and
            outputs a (modified) (list of) value(s).
        exceptions: Error or list(Error)
            Exception(s) which will be caught from the update function. Rows
            for which the update function raises one of the exceptions in this
            list will be placed in the data frame with failed rows.

        Raises
        ------
        ValueError
        """
        # Ensure that columns is a list
        self.columns = columns
        self.func = get_update_function(func=func, columns=self.columns)
        self.exceptions = exceptions if exceptions is not None else Exception

    def split(self, df):
        """Modify rows in the given data frame. Returns a modified data frame
        where values have been updated by the results of evaluating the
        associated row update function, and one data frame that contains the
        rows for which the update failed.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame, pandas.DataFrame
        """
        # Call the prepare method
        _, colidxs = select_clause(df=df.columns, columns=self.columns)
        # Call the prepare method of the update function.
        f = self.func.prepare(df)
        # Create two data frame, one with the successfully modified rows and
        # one with the rows for which the modification functions raised an
        # exception that was caught by the exception list. For each resulting
        # data frame we need to maintain the data and the row indices.
        success = dict({'data': list(), 'index': list()})
        fail = dict({'data': list(), 'index': list()})
        # Have different implementations for single column or multi-column
        # updates.
        if len(colidxs) == 1:
            colidx = colidxs[0]
            for rowid, values in df.iterrows():
                try:
                    val = f.eval(values)
                    values = list(values)
                    values[colidx] = val
                    success['data'].append(values)
                    success['index'].append(rowid)
                except self.exceptions:
                    fail['data'].append(list(values))
                    fail['index'].append(rowid)
        else:
            col_count = len(colidxs)
            for rowid, values in df.iterrows():
                try:
                    vals = f.eval(values)
                    if len(vals) != col_count:
                        msg = 'expected {} values instead of {}'
                        raise ValueError(msg.format(col_count, vals))
                    values = list(values)
                    for i in range(col_count):
                        values[colidxs[i]] = vals[i]
                    success['data'].append(values)
                    success['index'].append(rowid)
                except self.exceptions:
                    fail['data'].append(list(values))
                    fail['index'].append(rowid)
        # Create the two data frames for successful and failed modifications.
        df_succ = pd.DataFrame(
            data=success['data'],
            index=success['index'],
            columns=df.columns
        )
        df_fail = pd.DataFrame(
            data=fail['data'],
            index=fail['index'],
            columns=df.columns
        )
        return df_succ, df_fail
