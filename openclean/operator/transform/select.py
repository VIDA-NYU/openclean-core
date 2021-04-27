# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions and classes that implement the column selection operator in
openclean.
"""

from typing import Optional

import pandas as pd

from openclean.data.stream.base import DataRow
from openclean.data.types import Column, DatasetSchema
from openclean.data.schema import as_list, select_clause
from openclean.operator.base import Columns, DataFrameTransformer, Names
from openclean.operator.stream.consumer import StreamFunctionHandler
from openclean.operator.stream.processor import StreamProcessor


# -- Functions ----------------------------------------------------------------

def select(
    df: pd.DataFrame, columns: Columns, names: Optional[Names] = None
) -> pd.DataFrame:
    """Projection operator that selects a list of columns from a data frame.
    Returns a data frame that contains only thoses columns that are included in
    the given select clause. The optional list of names allows to rename the
    columns in the resulting data frame. If the list of names is given, it has
    to be of the same length as the list of columns.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string)
        Single column or list of column index positions or column names.
    names: string or list(string)
        Single name or list of names for the resulting columns.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return Select(columns=columns, names=names).transform(df)


# -- Operators ----------------------------------------------------------------

class Select(StreamProcessor, DataFrameTransformer):
    """Data frame transformer that selects a list of columns from a data frame.
    The output is a data frame that contains all rows from an input data frame
    but only those columns that are included in a given select clause.
    """
    def __init__(self, columns: Columns, names: Optional[Names] = None):
        """Initialize the list of columns that are being selected. The optional
        list of names allows to rename columns. If the list of names is given
        it has to be of the same length as the list of columns.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        names: string or list(string)
            Single name or list of names for the resulting columns.

        Raises
        ------
        ValueError
        """
        self.columns = as_list(columns)
        # Set the list of names for the result columns if given. Raise an error
        # if the list of column names is given but does not match the length of
        # the list of selected columns.
        if names is not None:
            self.names = names if isinstance(names, list) else [names]
            if len(self.columns) != len(self.names):
                raise ValueError('incompatible lists for columns and names')
        else:
            self.names = None

    def open(self, schema: DatasetSchema) -> StreamFunctionHandler:
        """Factory pattern for stream consumer. Returns an instance of a
        stream consumer that filters columns from data frame rows using the
        associated list of columns (i.e., the select clause).

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamFunctionHandler
        """
        # Get the names and index positions for the selected columns.
        colnames, colidxs = select_clause(schema=schema, columns=self.columns)
        # Adjust column indices.
        columns = list()
        for colidx in range(len(colnames)):
            col = colnames[colidx]
            colid = col.colid if isinstance(col, Column) else colidx
            columns.append(Column(colid=colid, name=col, colidx=colidx))

        def streamfunc(row: DataRow) -> DataRow:
            """Include only columns in the select clause."""
            return [row[i] for i in colidxs]

        return StreamFunctionHandler(columns=columns, func=streamfunc)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return a data frame that contains all rows but only those columns
        from the given input data frame that are included in the select clause.

        Raises a value error if the list of columns contains an item that
        cannot be matched to a column in the given data frame.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame

        Raises
        ------
        ValueError
        """
        # Ensure that all elements in the selected column list are names.
        colnames, colidx = select_clause(df.columns, columns=self.columns)
        result = df.iloc[:, colidx]
        # Rename columns if a list of new nmaes was given. Ensure to keep the
        # unique column identifier for all columns in the result.
        if self.names is not None:
            columns = list()
            for i in range(len(self.names)):
                col = colnames[i]
                if isinstance(col, Column):
                    col = Column(colid=col.colid, name=self.names[i])
                else:
                    col = self.names[i]
                columns.append(col)
            result.columns = columns
        else:
            result.columns = colnames
        return result
