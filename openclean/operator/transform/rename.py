# This file is part of the Data Cleaning Library (openclean).
#
# Copyright (C) 2018-2021 New York University.
#
# openclean is released under the Revised BSD License. See file LICENSE for
# full license details.

"""Functions and classes that implement the column renaming operator in
openclean.
"""

from openclean.data.types import Column, Columns, DatasetSchema
from openclean.data.schema import as_list
from openclean.operator.base import DataFrameTransformer
from openclean.util.core import scalar_pass_through
from openclean.operator.stream.consumer import StreamFunctionHandler
from openclean.operator.stream.processor import StreamProcessor


# -- Functions ----------------------------------------------------------------

def rename(df, columns, names):
    """The column rename operator returns a data frame where a given list of
    columns has been renamed. The renaming does not have to include all columns
    in the data frame. However, the given list of columns and new column names
    have to be of the same length.

    Parameters
    ----------
    df: pandas.DataFrame
        Input data frame.
    columns: int, string, or list(int or string)
        Single column or list of column index positions or column names.
    names: string or list(string)
        Single name or list of names for the renamed columns.

    Returns
    -------
    pandas.DataFrame

    Raises
    ------
    ValueError
    """
    return Rename(columns=columns, names=names).transform(df)


# -- Operators ----------------------------------------------------------------

class Rename(StreamProcessor, DataFrameTransformer):
    """Data frame transformer that renames a selected list of columns in a data
    frame. The output is a data frame that contains all rows and columns from
    an input data frame but with thoses columns that are listed in the given
    column list being renamed with the respective value in the given names
    list.
    """
    def __init__(self, columns: Columns, names: DatasetSchema):
        """Initialize the list of columns that are being renames and the list
        new column names. The length of both lists has to be equal. If scalar
        values are provided for either columns or names they are converted into
        lists.

        Parameters
        ----------
        columns: int, string, or list(int or string)
            Single column or list of column index positions or column names.
        names: string or list(string)
            Single name or list of names for the renamed columns.

        Raises
        ------
        ValueError
        """
        self.columns = as_list(columns)
        self.names = names if isinstance(names, list) else [names]
        # Raise an error if the length of the list of columns and column names
        # do not match.
        if len(self.columns) != len(self.names):
            raise ValueError('incompatible list for columns and names')

    def open(self, schema: DatasetSchema) -> StreamFunctionHandler:
        """Factory pattern for stream consumer. Returns an instance of a
        stream consumer that has a schema with renamed columns. The associated
        stream function does not manipulate any of the rows.

        Parameters
        ----------
        schema: list of string
            List of column names in the data stream schema.

        Returns
        -------
        openclean.operator.stream.consumer.StreamFunctionHandler
        """
        # Get schema with renamed columns.
        columns = self.rename(schema)
        return StreamFunctionHandler(columns=columns, func=scalar_pass_through)

    def rename(self, schema: DatasetSchema) -> DatasetSchema:
        """Create a modified dataset schema with renamed columns.

        Parameters
        ----------
        schema: list of string
            Dataset input schema.

        Returns
        -------
        list of string
        """
        # Start with a copy of columns in the data frame.
        renamed_columns = list(schema)
        # For each column in the columns list find the index position in the
        # data frame schema
        colidx = range(len(schema))
        for nidx in range(len(self.columns)):
            cidx = self.columns[nidx]
            if isinstance(cidx, str):
                # Find the first occurrence of a column with the given name.
                try:
                    cidx = next(i for i in colidx if schema[i] == cidx)
                except StopIteration:
                    raise ValueError('unknown column name {}'.format(cidx))
            # The variable cidx points to the column that is being renamed.
            try:
                col = schema[cidx]
            except IndexError as ex:
                raise ValueError(ex)
            if isinstance(col, Column):
                col = Column(colid=col.colid, name=self.names[nidx], colidx=col.colidx)
            else:
                col = self.names[nidx]
            renamed_columns[cidx] = col
        return renamed_columns

    def transform(self, df):
        """Return a data frame that contains all rows and columns from an input
        data frame but with thoses columns that are listed in the given column
        list being renamed with the respective value in the given names list.

        Parameters
        ----------
        df: pandas.DataFrame
            Input data frame.

        Returns
        -------
        pandas.DataFrame
        """
        # Get schema with renamed columns.
        renamed_columns = self.rename(df.columns)
        # Create a copy of the data frame and then replace columns with the
        # list of renamed columns
        result = df.copy()
        result.columns = renamed_columns
        return result
